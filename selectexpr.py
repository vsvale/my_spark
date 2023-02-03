from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.getOrCreate()

df = spark.read.json("data/device/device_2022_6_7_19_39_24.json")

df.printSchema()

rows = df.count()
cols = len(df.columns)
print("DataFrame Dimensions: {}x{}".format(rows,cols))

# select Expr
df.selectExpr("manufacturer","model","platform as type").show()

df.select(expr("CASE WHEN manufacturer = 'Xiamomi' THEN 1 ELSE 0 END").alias('Xiamomi')).show()