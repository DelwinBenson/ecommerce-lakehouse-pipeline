import pytest
from pyspark.sql import SparkSession
from validation import check_not_empty,check_required_columns,check_nulls

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("tests").getOrCreate()

def test_check_not_empty_raises_on_empty_df(spark):
    df=spark.createDataFrame([],"id INT")
    with pytest.raises(ValueError):
        check_not_empty(df) #should raise error

def test_check_empty_passes_on_empty_df(spark):
    df=spark.createDataFrame([(1,)],["id"])
    check_not_empty(df) #should not raise

def test_check_required_columns_raises_on_missing_column(spark):
    df=spark.createDataFrame([(1,"a")],["id","name"])
    with pytest.raises(ValueError):
        check_required_columns(df,["id","name","email"])#email is missing

def test_check_required_columns_passes_when_all_present(spark):
    df=spark.createDataFrame([(1,"a","x@y.com")],["id","name","email"])
    check_required_columns(df,["id","name","email"]) #should not raise

def test_check_nulls_raises_when_nulls_present(spark):
    df=spark.createDataFrame([(1,"a"),(None,"b")],["id","name"])
    with pytest.raises(ValueError):
        check_nulls(df,["id"])#should cause error

def test_check_nulls_passes_when_no_nulls(spark):
    df=spark.createDataFrame([(1,"a"),(2,"b")],["id","name"])
    check_nulls(df,["id"])#should not raise