from pyspark.sql import SparkSession
from pyspark import SparkFiles

spark = (SparkSession
        .builder
        .config("spark.memory.offHeap.enabled","true")
        .config("spark.memory.offHeap.size","100mb")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate())

url = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vct/vendas-oleo-diesel-tipo-m3-2013-2022.csv"
spark.sparkContext.addFile(url)

df = spark.read.csv(SparkFiles.get("*.csv"), header=True, sep=";")

df.show()

df.printSchema()

rows = df.count()
cols = len(df.columns)
print("DataFrame Dimensions: {}x{}".format(rows,cols))