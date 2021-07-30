from pyspark.sql import SparkSession
import sys

num_cores = "local[" + sys.argv[2] + "]"
# print ("num_cores  : ", num_cores)
spark = SparkSession \
    .builder \
    .master(num_cores)\
    .appName("DS_A_5") \
    .getOrCreate()

df = spark.read.csv("airports.csv", header=True)

out_file = sys.argv[1]

df_new = df.groupBy("COUNTRY").count().withColumnRenamed("count", "# Airports")
df_new.toPandas().to_csv(out_file, header=True,index=False)