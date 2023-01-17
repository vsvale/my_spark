from pyspark.sql import SparkSession

spark = (SparkSession
        .builder
        .config("spark.memory.offHeap.enabled","true")
        .config("spark.memory.offHeap.size","100mb")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate())

# data import
spark.sql(
    """
    CREATE TEMPORARY VIEW vw_device
    USING org.apache.spark.sql.json
    OPTIONS (path "data/device/*.json")
    """
)

spark.sql(
    """
    CREATE TEMPORARY VIEW vw_subscription
    USING org.apache.spark.sql.json
    OPTIONS (path "data/subscription/*.json")
    """
)

# list temp views
print(spark.catalog.listTables())

# select data
spark.sql("""SELECT * FROM vw_device LIMIT 10;""").show()
spark.sql("""SELECT * FROM vw_subscription LIMIT 10;""").show()

# join
join_df = spark.sql("""
SELECT 
    dev.user_id,
    dev.model,
    dev.platform,
    subs.payment_method,
    subs.plan
FROM vw_device AS dev
INNER JOIN vw_subscription AS subs
ON dev.user_id = subs.user_id
""")

# schema
join_df.printSchema()

# dimensions
rows = join_df.count()
cols = len(join_df.columns)
print("DataFrame Dimensions: {}x{}".format(rows,cols))
