# imports
from pyspark.sql import SparkSession
from pyspark import SparkConf

from utils.extract import read_json
from utils.transform import get_greatest_int,get_rate
from utils.load import write_into_parquet


def main():
    spark = (SparkSession
        .builder
        .config("spark.memory.offHeap.enabled","true")
        .config("spark.memory.offHeap.size","100mb")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate())

    print(spark)
    print(SparkConf().getAll())
    spark.sparkContext.setLogLevel("INFO")

    # Extract
    device_filepath = 'data/device/'
    df = read_json(spark=spark, filepath=device_filepath)
    df.show()

    # Transform
    rank = get_greatest_int(df=df)
    rank.show()
    df_transformed = get_rate(df=df)

    # Load
    write_into_parquet(df=df_transformed, mode="overwrite", location="data/output/parquet")

if __name__ == '__main__':
    main()