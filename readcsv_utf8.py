from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, current_date

spark = (SparkSession
        .builder
        .config("spark.memory.offHeap.enabled","true")
        .config("spark.memory.offHeap.size","100mb")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate())

# load json data
df = (spark
    .read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .option("delimiter",";")
    .option("encoding", "ISO-8859-1")
    .load("data/veiculos/veiculos_*.csv")
)

df = df.withColumn("processed_at", current_timestamp())
df = df.withColumn("load_date", current_date())

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

