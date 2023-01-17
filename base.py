# import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, max, min, date_format,current_timestamp, current_date, lit, datediff, when, from_unixtime
#https://spark.apache.org/docs/latest/sql-ref-datatypes.html
from pyspark.sql.types import StructType,StructField, StringType, LongType


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

# select columns
(
    df
    .select(
        col("manufacturer"),
        col("model"),
        col("platform").alias("type")
        )
    .filter(col("manufacturer")=="Xiamomi")
    .groupBy(col("manufacturer"),col("model"))
    .count()
    .orderBy("count",ascending=False)
    .limit(10)
    .show(truncate=False)
)

# transformations
(
    df.select(
        mean("id"),
        max("id"),
        min("id"),
        date_format(current_timestamp(),'yyyMM'),
        date_format(current_timestamp(),'yyyMM').cast('int').alias('anomes'),
        lit(None).alias("Null")
        )
        .show()
)

(
    df.select(
        datediff(from_unixtime(col('dt_current_timestamp')),current_timestamp()).alias('diffdates'),
        when(col('manufacturer').isNull(),lit('N/A')).otherwise(col('manufacturer')),
        when(col('manufacturer').isNotNull(),col('manufacturer')).otherwise(col('N/A'))
    )
    .show()
)

# drop duplicates
print("Non-duplicates lines",
    df.dropDuplicates().count()
)

# describe
df.describe().show(truncate=False)

# execution plan
df.explain(mode="formatted")

# processed at
df = df.withColumn("processed_at", current_timestamp())
df = df.withColumn("load_date", current_date())


# spark-submit base.py