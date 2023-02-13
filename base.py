# import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, max, min, date_format,current_timestamp, current_date, lit, datediff, when, from_unixtime, monotonically_increasing_id,greatest, round, year, first, last
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
        .config("spark.sql.files.maxPartitionBytes","128Mb")
        .getOrCreate())

# Even read.json infer schema, is good practice to enforce a schema
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

# using * wild card to read all files with same pattern
df = spark.read.json("data/device/device_*json",schema=schema)

# cache if gonna use it more than once
df.cache()

# printSchema
df.printSchema()

# verify data types
print(df.dtypes)

# dimensions
rows = df.count()
cols = len(df.columns)
print("DataFrame Dimensions: {}x{}".format(rows,cols))

# number of partitions
df.rdd.getNumPartitions()

# https://github.com/palantir/pyspark-style-guide
# first df select, alias, trim, filter
# secound df add columns and transform
# third df join and drop columns
is_xiamomi = (col("manufacturer")=="Xiamomi")
(
    df
    .select(
        "manufacturer",
        "model",
        col("platform").alias("type")
        )
    .filter(is_xiamomi)
    .groupBy(col("manufacturer"),col("model"))
    .count()
    .orderBy("count",ascending=False)
    .limit(10)
    .show(truncate=False)
)

# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html

(
    df.select(
        mean("id"),
        max("id"),
        min("id"),
        date_format(current_timestamp(),'yyyMM'),
        date_format(current_timestamp(),'yyyMM').cast('int').alias('anomes'),
        year(current_timestamp()),
        lit(None).alias("Null"),
        first('id',ignorenulls=True),
        last('id',ignorenulls=True)
        )
        .show()
)

(
    df.select(
        monotonically_increasing_id().alias("autogenid"),
        datediff(from_unixtime(col('dt_current_timestamp')),current_timestamp()).alias('diffdates'),
        when(col('manufacturer').isNull(),lit(None)).otherwise(col('manufacturer')),
        when(col('manufacturer').isNotNull(),col('manufacturer')).otherwise(lit(None)),
        when(col('manufacturer')=="Xiamomi",lit(1)).otherwise(lit(None)),
        greatest("id","build_number","version","user_id").alias("highest_number"),
        round(col("version"),0)
    )
    .show()
)

# filter and when max 3 expressions at most
has_null = ((col('manufacturer').isNull()) | (col('version').isNull()))
is_bad = (has_null | is_xiamomi)

(
    df.select(
        when(is_bad,'N/A')
    )
    .show()
)

# never sue withCOlumn to rename or cast
df.withColumn("rank",when(col("manufacturer")=="Xiamomi",lit("low")).otherwise(lit("normal")))

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

# in join avoid join explosion and right joins

subscription = spark.read.json("data/device/subscription_*json",schema=schema)


# Use function to represent atomic logic steps
# One function should not be over 70 lines
def join_device_subscription(df, subscription):
    device = df.alias('device').select('manufacturer','platform','id')
    
    subscription = subscription.alias('subscription').select('id','plan')

    devic_subs = device.join(subscription, col('device.id')==col('subscription.id'), how='inner')

    devic_subs = (
    devic_subs.select(
        col('device.manufacturer').alias('manufacturer'),
        col('device.platform').alias('platform'),
        col('subscription.plan').alias('plan')
    )   
    )
    return devic_subs


# Save as 1 file
df.repartition(1).write.csv(path='csv',mode='overwrite',sep=';',header=True)

# Save as Orc File
df.write.orc(path='orc',mode='overwrite')


#spark-submit --deploy-mode client --master local \
#--driver-memory 1GB --driver-cores 2 --num-executors 2 --executor-memory 1GB --executor-cores 1 --total-executor-cores 2 \
#--conf "spark.sql.shuffle.partitions=20000" \
#--jars "dependency1.jar" \
#base.py

# deploy-mode cluster | client (locally, default)
# master yarn | mesos://host:port| spark://host:port | k8s://host:port | local
