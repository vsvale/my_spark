import great_expectations as ge
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from pyspark.sql.functions import col, monotonically_increasing_id, lit, when
from pyspark.sql import Windown
import json

spark = (SparkSession
        .builder
        .config("spark.memory.offHeap.enabled","true")
        .config("spark.memory.offHeap.size","100mb")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes","128Mb")
        .getOrCreate())

df = (
    spark
    .read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .option("delimiter",",")
    .option("encoding", "ISO-8859-1")
    .load("data/bank/bank-full.csv")
)

df = (
    df
    .withColumn('job', when(col('job') == 'unknown', lit(None)).otherwise(col('job')))
    .withColumn('id', monotonically_increasing_id())
)