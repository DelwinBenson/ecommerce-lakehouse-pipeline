from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
from utils.config import BRONZE_TABLES,SILVER_TABLES
from utils.logging_config import get_logger
from validation import check_referential_integrity

logger=get_logger(__name__)

def transform_customers()->None:
    df=spark.read.table(BRONZE_TABLES["customers"])
    initial_count=df.count()

    window=Window.partitionBy("customer_id").orderBy(F.col("ingestion_timestamp").desc())
    df=(
        df.withColumn("row_num",F.row_number().over(window))
            .filter(F.col("row_num")==1)
            .drop("row_num")
            .withColumn("signup_date",F.to_date("signup_date"))
    ).cache()

    final_count=df.count()
    logger.info(f"customers: {initial_count}->{final_count} rows after dedup+filter")

    df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["customers"])
    df.unpersist()

def transform_products()->None:
    df=spark.read.table(BRONZE_TABLES["products"])
    initial_count=df.count()

    window=Window.partitionBy("product_id").orderBy(F.col("ingestion_timestamp").desc())

    df=(
        df.withColumn("row_num",F.row_number().over(window))
            .filter(F.col("row_num")==1)
            .drop("row_num")
            .filter(F.col("price")>0)
            .withColumn("price",F.col("price").cast(DecimalType(10,2)))
    ).cache()

    final_count=df.count()
    logger.info(f"products: {initial_count}->{final_count} rows after dedup+filter")

    df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["products"])
    df.unpersist()

def transform_orders()->None:
    df=spark.read.table(BRONZE_TABLES["orders"])
    initial_count=df.count()

    window=Window.partitionBy("order_id").orderBy(F.col("ingestion_timestamp").desc())
    df=(
        df.withColumn("row_num",F.row_number().over(window))
            .filter(F.col("row_num")==1)
            .drop("row_num")
            .withColumn("order_date",F.to_date("order_date"))
    )

    final_count=df.count()
    logger.info(f"orders: {initial_count}->{final_count} rows after dedup+filter")

    #referential integrity checks
    df=check_referential_integrity(df,SILVER_TABLES["customers"],"customer_id",table_name="orders")

    df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["orders"])
    

def transform_order_items()->None:
    df=spark.read.table(BRONZE_TABLES["order_items"])
    initial_count=df.count()

    window=Window.partitionBy("order_item_id").orderBy(F.col("ingestion_timestamp").desc())
    df=(
        df.withColumn("row_num",F.row_number().over(window))
            .filter(F.col("row_num")==1)
            .drop("row_num")
            .filter(F.col("price")>0)
            .filter(F.col("qty")>0)
            .withColumn("price",F.col("price").cast(DecimalType(10,2)))
            .withColumn("total_price",(F.col("qty")*F.col("price")).cast(DecimalType(10,2)))
    )

    final_count=df.count()
    logger.info(f"order_items: {initial_count}->{final_count} rows after dedup+filter")

    #referential integrity checks
    df=check_referential_integrity(df,SILVER_TABLES["orders"],"order_id",table_name="order_items")
    df=check_referential_integrity(df,SILVER_TABLES["products"],"product_id",table_name="order_items")
    
    df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["order_items"])


def transform_payments()->None:
    df=spark.read.table(BRONZE_TABLES["payments"])
    initial_count=df.count()

    window=Window.partitionBy("payment_id").orderBy(F.col("ingestion_timestamp").desc())
    df=(
        df.withColumn("row_num",F.row_number().over(window))
            .filter(F.col("row_num")==1)
            .drop("row_num")
            .filter(F.col("amount")>0)
            .withColumn("amount",F.col("amount").cast(DecimalType(10,2)))
        )

    final_count=df.count()
    logger.info(f"payments: {initial_count}->{final_count} rows after dedup+filter")

    #referential integrity checks
    df=check_referential_integrity(df,SILVER_TABLES["orders"],"order_id",table_name="payments")

    df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["payments"])

def transform_shipments()->None:
    df=spark.read.table(BRONZE_TABLES["shipments"])
    initial_count=df.count()

    window=Window.partitionBy("shipment_id").orderBy(F.col("ingestion_timestamp").desc())
    df=(
        df.withColumn("row_num",F.row_number().over(window))
            .filter(F.col("row_num")==1)
            .drop("row_num")
            .withColumn("status",F.lower(F.trim(F.col("status"))))
            .filter(F.col("status").isin(["shipped","delivered","late"]))
    )

    final_count=df.count()
    logger.info(f"shipments: {initial_count}->{final_count} rows after dedup+filter")

    #referential integrity checks
    df=check_referential_integrity(df,SILVER_TABLES["orders"],"order_id",table_name="shipments")

    df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["shipments"])

def transform_returns()->None:
    df=spark.read.table(BRONZE_TABLES["returns"])
    initial_count=df.count()

    window=Window.partitionBy("return_id").orderBy(F.col("ingestion_timestamp").desc())
    df=(
        df.withColumn("row_num",F.row_number().over(window))
            .filter(F.col("row_num")==1)
            .drop("row_num")
            .filter(F.col("refund")>0)
            .withColumn("refund",F.col("refund").cast(DecimalType(10,2)))
    )

    final_count=df.count()
    logger.info(f"returns: {initial_count}->{final_count} rows after dedup+filter")

    #referential integrity checks
    df=check_referential_integrity(df,SILVER_TABLES["order_items"],"order_item_id",table_name="returns")

    df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["returns"])

#each table_name maps to (transform_func,[list of tables it depends on])
silver_functions={
    "customers": (transform_customers,[]),
    "products": (transform_products,[]),
    "orders": (transform_orders,["customers"]),
    "order_items": (transform_order_items,["orders","products"]),
    "payments": (transform_payments,["orders"]),
    "shipments": (transform_shipments,["orders"]),
    "returns": (transform_returns,["order_items"])
}
completed=set()
failures={}

for table_name,(func,depends_on)in silver_functions.items():
    missing_deps=[d for d in depends_on if d not in completed]
    if missing_deps:
        failures[table_name]=f"Skipped-missing dependencies: {missing_deps}"
        logger.error(f"Skipped {table_name}, missing dependencies: {missing_deps}")
        continue

    try:
        func()
        completed.add(table_name)
        logger.info(f"Transformed {table_name} successfully")
    except Exception as e:
        failures[table_name]=str(e)
        logger.error(f"Failed to tranform {table_name} : {e}")

if failures:
    raise RuntimeError(f"Silver transformation failed for: {list(failures.keys())}")