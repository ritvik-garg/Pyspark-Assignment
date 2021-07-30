from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import col
import pandas as pd
import sys
import numpy as np

num_cores = "local[" + sys.argv[2] + "]"
# print ("num_cores  : ", num_cores)
spark = SparkSession \
    .builder \
    .master(num_cores)\
    .appName("DS_A_5") \
    .getOrCreate()

df = spark.read.csv("airports.csv", header=True)
out_file = sys.argv[1]

df_new = df.groupBy("COUNTRY").count().sort(col("count").desc())
df_new = df_new.toPandas().iloc[0,0]

np_arr = np.array([df_new])
print ("df_2 : ", np_arr, np_arr.shape)


df_new = pd.DataFrame(np_arr)   
df_new.to_csv(out_file, header=False,index=False)