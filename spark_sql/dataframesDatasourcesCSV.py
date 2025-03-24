# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

data_path = "C:\\PySpark\\data\\flight-data\\csv\\2010-summary.csv"
data_output_path = "C:\\PySpark\\data\\flight-data\\csv_out\\"

spark = SparkSession \
    .builder \
    .appName("Datasorces") \
    .config("spark.master", "local") \
    .getOrCreate()


# other read formats:  PERMISSIVE, DROPMALFORMED
csvFile = spark.read.format("csv")\
  .option("header", "true")\
  .option("mode", "FAILFAST")\
  .option("inferSchema", "true")\
  .load(data_path)
  
filterQry = col("DEST_COUNTRY_NAME") == "United States" 
csvFiltered = csvFile.where(filterQry)  
  
csvFiltered.show()

csvFiltered.limit(20).write.format("parquet")\
  .mode("append")\
  .save("C:\\PySpark\\data\\flight-data\\parquet_out\\")
  
#type(csvFile)  

spark.stop()