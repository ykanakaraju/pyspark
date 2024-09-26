# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

data_path = "C:\\PySpark\\data\\flight-data\\parquet\\2010-summary.parquet"
data_output_path = "C:\\PySpark\\data\\flight-data\\parquet_out\\"

spark = SparkSession \
    .builder \
    .appName("Datasorces") \
    .config("spark.master", "local") \
    .getOrCreate()

parquetFile = spark.read.format("parquet").load(data_path)
  
parquetFile.show()

filterQry = col("count") > 100
parquetFiltered = parquetFile.where(filterQry)  

parquetFiltered.write.format("parquet").save(data_output_path)

spark.stop()