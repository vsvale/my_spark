from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, sha2, concat_ws, count
from delta import *
from delta.tables import *
from pyspark.sql.functions import current_timestamp, current_date
from pyspark.sql.types import StructType,StructField, StringType, LongType

builder = (SparkSession.builder.appName("delta-example")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.memory.offHeap.enabled","true")
    .config("spark.memory.offHeap.size","100mb")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.parquet.enableVectorizedReader","false")

)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

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

df = spark.read.json("data/device/device_*json",schema=schema)

df = df.withColumn("processed_at", current_timestamp())
df = df.withColumn("load_date", current_date())

destination = "data/output/delta"
write_delta_mode = "overwrite"

# write delta
if DeltaTable.isDeltaTable(spark, destination):
    dt_table = DeltaTable.forPath(spark, destination)
    dt_table.alias("historical_data")\
            .merge(
                df.alias("new_data"),
                '''
                historical_data.id = new_data.id''')\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()
else:
    df.write.mode(write_delta_mode)\
            .format("delta")\
            .partitionBy("dt_current_timestamp")\
            .save(destination)

# read delta
origin_count = df.count()

destiny = spark.read \
        .format("delta") \
        .load(destination)

destiny_count = destiny.count()

if origin_count != destiny_count:
    raise AssertionError("Counts of origin and destiny are not equal")

# stop session
spark.stop()