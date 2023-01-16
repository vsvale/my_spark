from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.json("data/device/device_2022_6_7_19_39_24.json")

df.printSchema()

rows = df.count()
cols = len(df.columns)
print("DataFrame Dimensions: {}x{}".format(rows,cols))

# select Expr
df.selectExpr("manufacturer","model","platform as type").show()