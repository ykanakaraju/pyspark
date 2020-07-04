# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession

data_path = "C:\\PySpark\\data\\"

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Introduction") \
    .config("spark.master", "local") \
    .getOrCreate()

flightData = spark \
  .read \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .csv("C:/PySpark/data/flight-data/csv/2015-summary.csv")
  
flightData.printSchema()
  
# Check how many partitions are created for flightData  
flightData.explain()  
print("1. Number of partitions for flightData: {}".format( flightData.rdd.getNumPartitions() ) )  

flightDataSorted = flightData.sort("count")
#flightDataSorted.explain()  
print("2. Number of partitions for flightDataSorted: {}".format( flightDataSorted.rdd.getNumPartitions() ) )  

# Change the number of partitions
spark.conf.set("spark.sql.shuffle.partitions", "5")
