from pyspark.sql import SparkSession

spark = (SparkSession
        .builder
        .config("spark.memory.offHeap.enabled","true")
        .config("spark.memory.offHeap.size","100mb")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes","128Mb")
        .getOrCreate())

# Read hive table
df = spark.table('base_operacional.pagamento_consolidado').alias('pgc')

# Refresh table
spark.catalog.refreshTable("base_operacional.pagamento_consolidado")

# Truncate Table
spark.sql("TRUNCATE TABLE base_operacional.pagamento_consolidado")

# Write Table
df.write.format('orc').option('orc.compress','zlib').mode('overwrite').insertInto("base_operacional.pagamento_consolidadoo", overwrite=True)