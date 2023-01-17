from pyspark.sql import SparkSession

spark = (SparkSession
        .builder
        .config("spark.memory.offHeap.enabled","true")
        .config("spark.memory.offHeap.size","100mb")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate())

import pyspark.pandas as ps

df_device = ps.read_json("data/device/*.json")
df_subscription = ps.read_json("data/subscription/*.json")

print(df_device.head(10))

df_device.info()

df_device.spark.explain(mode="formatted")

