from pyspark.sql import functions as F
from utils.config import SILVER_TABLES,GOLD_TABLES
from validation import check_unique
from utils.logging_config import get_logger

logger=get_logger(__name__)

def create_fact_order_items()-> None:
    order_items=spark.read.table(SILVER_TABLES["order_items"])
    orders=spark.read.table(SILVER_TABLES["orders"])
    order_items_count=order_items.count()

    order_items.createOrReplaceTempView("order_items")
    orders.createOrReplaceTempView("orders")

    fact_order_items=spark.sql(
        """
        select
            oi.order_item_id,
            oi.order_id,
            oi.product_id,
            o.customer_id,
            o.order_date,
            date_format(o.order_date,'yyyy-MM')as order_month,
            oi.qty,
            oi.price,
            oi.total_price
        from order_items oi
        inner join orders o
            on oi.order_id=o.order_id
        """
    )

    final_count=fact_order_items.count()
    logger.info(f"fact_order_items: {final_count} rows created (source order_items had {order_items_count} rows)")
    if final_count!=order_items_count:
        logger.error(f"fact_order_items row count mismatch -- join dropped rows unexpectedly")
        raise ValueError(f"Row count mismatch: expected {order_items_count}, got {final_count}")

    fact_order_items.write.format("delta").mode("overwrite").partitionBy("order_month").saveAsTable(GOLD_TABLES["fact_order_items"])

def create_dim_products()->None:
    products=spark.read.table(SILVER_TABLES["products"])
    products.createOrReplaceTempView("products")

    dim_products=spark.sql(
        """
        select 
            product_id, 
            category_id,
            supplier_id,
            price
        from products
        """
    )

    check_unique(dim_products,"product_id")
    dim_products.write.format("delta").mode("overwrite").clusterBy("product_id").saveAsTable(GOLD_TABLES["dim_products"])

def create_dim_customers()->None:
    customers=spark.read.table(SILVER_TABLES["customers"])
    customers.createOrReplaceTempView("customers")

    dim_customers=spark.sql(
        """
        select 
            customer_id, 
            city,
            signup_date,
            date_format(signup_date, "yyyy-MM") as signup_month
        from customers
        """
    )

    check_unique(dim_customers,"customer_id")
    dim_customers.write.format("delta").mode("overwrite").clusterBy("customer_id").saveAsTable(GOLD_TABLES["dim_customers"])

def create_dim_date()->None:
    dim_date=spark.sql(
        """
        select
            explode(sequence(to_date('2020-01-01'),to_date('2024-01-31'),interval 1 day)) as date
        """
    )
    dim_date=dim_date.select(
        F.col("date"),
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        F.date_format("date","MMMM").alias("month_name"),
        F.date_format("date","EEEE").alias("day_name"),
        F.quarter("date").alias("quarter"),
        F.date_format("date","yyyy-MM").alias("year_month")
    )

    dim_date.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLES["dim_date"])

gold_functions={
    "fact_order_items": create_fact_order_items,
    "dim_products": create_dim_products,
    "dim_customers": create_dim_customers,
    "dim_date": create_dim_date
}

failures={}
for table_name,func in gold_functions.items():
    try:
        func()
        logger.info(f"{table_name} created successfully")
    except Exception as e:
        failures[table_name]=str(e)
        logger.error(f"Failed in creating {table_name} : {e}")

if failures:
    raise RuntimeError(f"Gold transformation failed for: {list(failures.keys())}")   

