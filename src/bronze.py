from utils.logging_config import get_logger
from utils.config import SOURCE_PATH,BRONZE_TABLES,load_bronze_config
from validation import check_not_empty,check_nulls,check_required_columns
from pyspark.sql import functions as F

logger=get_logger(__name__)
BRONZE_CONFIG=load_bronze_config()

def ingest_bronze(table_name:str)->None:
    source_path=f"{SOURCE_PATH}/{table_name}.csv"
    target_table=BRONZE_TABLES[table_name]
    table_config=BRONZE_CONFIG[table_name]

    df=(
        spark.read.format("csv")\
            .option("header",True)\
            .option("inferSchema",True)\
            .load(source_path)
    )

    check_not_empty(df)
    check_required_columns(df,table_config["required_columns"])
    check_nulls(df,table_config["key_columns"])

    df=df.withColumn("ingestion_timestamp",F.current_timestamp())

    (
        df.write\
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
    )

failures={}
for table_name in BRONZE_TABLES:
    try:
        ingest_bronze(table_name)
        logger.info(f"Ingested {table_name} successfully")
    except Exception as e:
        failures[table_name]=str(e)
        logger.error(f"Failed to ingest {table_name}: {e}")

if failures:
    raise RuntimeError(f"Bronze ingestion failed for: {list(failures.keys())}")
