# import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
#https://spark.apache.org/docs/latest/sql-ref-datatypes.html
from pyspark.sql.types import StructType,StructField, StringType, LongType
from pyspark.sql.functions import current_timestamp, current_date


# SparkSession for dataframes and datasets
# SparkContext for RDD

# init spark session
spark = (SparkSession
        .builder
        .config("spark.memory.offHeap.enabled","true")
        .config("spark.memory.offHeap.size","100mb")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate())

# schema
schema = (
    StructType([
        StructField("build_number",LongType(),True),
        StructField("dt_current_timestamp",LongType(),True),
        StructField("id",LongType(),True),
        StructField("manufacturer",StringType(),True),
        StructField("model",StringType(),True),
        StructField("platform",StringType(),True),
        StructField("serial_number",StringType(),True),
        StructField("uid",StringType(),True),
        StructField("user_id",LongType(),True),
        StructField("version",LongType(),True)
    ])
)

# load json data
df = spark.read.json("data/device/device_*json",schema=schema)

# cache if gonna use it more than once
df.cache()

# schema
df.printSchema()

# dtypes
print(df.dtypes)

# dimensions
rows = df.count()
cols = len(df.columns)
print("DataFrame Dimensions: {}x{}".format(rows,cols))

# show dataframe
df.show(truncate=False)

# select columns
(
    df
    .select(
        col("manufacturer"),
        col("model"),
        col("platform")
        )
    .filter(col("manufacturer")=="Xiamomi")
    .groupBy(col("manufacturer"))
    .count()
    .show(truncate=False)
)

# drop duplicates
print("Non-duplicates lines",
    df.dropDuplicates().count()
)

# describe
df.describe().show(truncate=False)

# processed at
df = df.withColumn("processed_at", current_timestamp())
df = df.withColumn("load_date", current_date())


# spark-submit base.py