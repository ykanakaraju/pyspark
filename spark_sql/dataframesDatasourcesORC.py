# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

data_path = "C:\\PySpark\\data\\flight-data\\orc\\2010-summary.orc"
data_output_path = "C:\\PySpark\\data\\flight-data\\orc_out\\"

spark = SparkSession \
    .builder \
    .appName("Datasorces") \
    .config("spark.master", "local") \
    .getOrCreate()

orcFile = spark.read.format("orc").load(data_path)
  
filterQry = col("count") > 100
orcFiltered = orcFile.where(filterQry)  
  
orcFiltered.write.format("orc")\
  .mode("overwrite")\
  .save(data_output_path)


spark.stop()