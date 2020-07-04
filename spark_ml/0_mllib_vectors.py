import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("Spark MLlib") \
        .config("spark.master", "local") \
        .getOrCreate()

######################################################
#   Example 1 - Dense & Sparse Vectors
######################################################
        
from pyspark.ml.linalg import Vectors
denseVec = Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
size = 12
idx = [1, 2, 10, 11] # locations of non-zero elements in vector
values = [12.0, 32.0, 110.0, 27.0]
sparseVec = Vectors.sparse(size, idx, values)

print("denseVec: ", denseVec)
print("sparseVec: ", sparseVec)

spark.stop()
