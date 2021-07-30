from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import col
import pandas as pd
import sys
import numpy as np


out_file = sys.argv[1]
# n_cpu_cores = sys.argv[2]

num_cores = "local[" + sys.argv[2] + "]"
# print ("num_cores  : ", num_cores)
spark = SparkSession \
    .builder \
    .master(num_cores)\
    .appName("DS_A_5") \
    .getOrCreate()

df = spark.read.csv("airports.csv", header=True)

df_new = df.filter((df['LATITUDE'] >=10) & (df['LATITUDE'] <=90) & (df['LONGITUDE'] <=-10) & (df['LONGITUDE'] >=-90)).select("NAME")
df_new.toPandas().to_csv(out_file, header=True,index=False)
