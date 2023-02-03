import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
from chispa import assert_column_equality
from chispa.dataframe_comparer import assert_df_equality

spark = (SparkSession
        .builder
        .config("spark.memory.offHeap.enabled","true")
        .config("spark.memory.offHeap.size","100mb")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate())

def remove_non_word_characters(df_column):
    return regexp_replace(df_column, "[^\\w\\s]+", "")

data = [
        ("matt7",),
        ("jo&&se",),
        ("**li**",),
        ("#::luisa",),
        (None,)
    ]
actual_df = spark.createDataFrame(data, ["name"])

expected = [
        ("matt7", "matt"),
        ("jo&&se", "jose"),
        ("**li**", "li"),
        ("#::luisa", "luisa"),
        (None, None)
    ]
expected_df = spark.createDataFrame(expected, ["name", "expected_name"])


#Always test None clause
def test_remove_non_word_characters(df):
    df = df.withColumn("clean_name", remove_non_word_characters(col("name")))
    assert_column_equality(df, "clean_name", "expected_name")

def test_df_equality(actual_df,expected_df):
    actual_df = actual_df.withColumn("clean_name", remove_non_word_characters(col("name")))
    assert_df_equality(actual_df, expected_df,ignore_row_order=True,ignore_column_order=True,ignore_nullable=True, allow_nan_equality=True)
    

#test_remove_non_word_characters(expected_df)
#test_df_equality(actual_df,expected_df)