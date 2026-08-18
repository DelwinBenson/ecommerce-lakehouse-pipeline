from pyspark.sql import DataFrame,SparkSession
from pyspark.sql import functions as F
from utils.logging_config import get_logger

spark = SparkSession.builder.getOrCreate()

logger=get_logger(__name__)

def check_not_empty(df:DataFrame)->None:
    """Fails if the DataFrame contains no rows"""
    if df.limit(1).count()==0:
        raise ValueError("DataFrame is empty")

def check_required_columns(df:DataFrame,required_columns:list[str])->None:
    """Fails if required columns are missing"""
    missing_columns=set(required_columns)-set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

def check_nulls(df:DataFrame,columns:list[str])->None:
    "Fails if specified columns contain nulls, particularly used for primary key columns"
    null_counts=(
        df.select([
            F.sum(F.when(F.col(c).isNull(),1).otherwise(0))
            .alias(c)
            for c in columns
        ])
        .collect()[0]
        .asDict()
    ) #counts nulls in specified columns

    invalid_columns={
        column:count
        for column,count in null_counts.items()
        if count>0
    } #dictionary comprehension to create a dict of invalid columns

    if invalid_columns:
        raise ValueError(
            f"Null values found: {invalid_columns}"
        )

def check_unique(df:DataFrame,column:str)->None:
    """Fails if column contains duplicate values"""
    total_count=df.count()
    distinct_count=df.select(column).distinct().count()

    if total_count!=distinct_count:
        raise ValueError(
            f"Duplicate values found in column: {column}"
        )

def check_referential_integrity(child_df:DataFrame,parent_table:str,fk_col:str,pk_col: str="",table_name: str="")->DataFrame:
    pk_col=pk_col or fk_col
    valid_keys=spark.read.table(parent_table).select(pk_col).distinct()
    before=child_df.count()
    result=(child_df.join(F.broadcast(valid_keys),child_df[fk_col]==valid_keys[pk_col],"inner").select(child_df["*"]))
    after=result.count()
    logger.info(f"{table_name}: {before} -> {after} rows after FK check on {fk_col}")
    return result
