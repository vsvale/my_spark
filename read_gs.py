from pyspark.sql import SparkSession
from pyspark import SparkConf

spark = (SparkSession
        .builder()
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile","data/keys/service-account-key.json")
        .getOrCreate()
)

print(SparkConf().getAll())
spark.sparkContext.setLogLevel("INFO")

df = spark.read.json("gs://bucekt-landing/device/*.json")
df.show()