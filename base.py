# import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
#https://spark.apache.org/docs/latest/sql-ref-datatypes.html
from pyspark.sql.types import StructType,StructField, StringType, LongType 


# SparkSession for dataframes and datasets
# SparkContext for RDD

# init spark session
spark = SparkSession.builder.getOrCreate()

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

# schema
df.printSchema()

# dtypes
print(df.dtypes)

# dimensions
rows = df.count()
cols = len(df.columns)
print("DataFrame Dimensions: {}x{}".format(rows,cols))

# show dataframe
df.show()

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



# spark-submit base.py