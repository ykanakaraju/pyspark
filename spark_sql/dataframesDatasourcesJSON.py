# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

data_path = "C:\\PySpark\\data\\flight-data\\json\\2010-summary.json"
data_output_path = "C:\\PySpark\\data\\flight-data\\json_out\\"

spark = SparkSession \
    .builder \
    .appName("Datasorces") \
    .config("spark.master", "local") \
    .getOrCreate()

jsonFile = spark.read.format("json")\
  .option("mode", "FAILFAST")\
  .option("inferSchema", "true")\
  .load(data_path)
  
filterQry = col("count") > 100
jsonFiltered = jsonFile.where(filterQry)  
  
jsonFiltered.write.format("json")\
  .mode("overwrite")\
  .save(data_output_path)



spark.stop()