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
    
######################################################    
#  Reading from Programmatic data
######################################################   

# Dataframe internally stores a Ror object    
myRange = spark.range(1000)
myRange.printSchema()
myRange.take(5)  # in this case, the column is unnamed (default: id)
myRange.schema

myRangeDf = myRange.toDF("number")   
myRangeDf.printSchema()
myRangeDf.take(5)
#myRangeDf.select(myRangeDf["number"] + 10)

myRangeEvenNumbers = myRangeDf.where("number % 2 = 0")
myRangeEvenNumbers.take(10)
myRangeEvenNumbers.show(10)
myRangeEvenNumbers.printSchema()
myRangeEvenNumbers.explain()


myRangeEvenNumbersList = myRangeEvenNumbers.take(20)

######################################################    
#  Reading from a CSV file
###################################################### 
 
flightData2015 = spark \
  .read \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .csv("C:/PySpark/data/flight-data/csv/2015-summary.csv")

##############################################
# dataframe lineage using explain
##############################################
  
# Let's apply a transformation and check the lineage
flightData2015Sorted = flightData2015.sort("count")
flightData2015Sorted.printSchema()
flightData2015Sorted.explain()
# Here, the dataframe is created with 200 partitions
# as shown in the explain plan - rangepartitioning(.., 200)

# We can explicitly set shuffle partitions using the following
spark.conf.set("spark.sql.shuffle.partitions", "5")
flightData2015Sorted2 = flightData2015.sort("count")
flightData2015Sorted2.explain()


##############################################
# Using datafrane API to process dataframe
##############################################

dfUsingDataframes = flightData2015 \
  .groupBy("DEST_COUNTRY_NAME") \
  .count()

dfUsingDataframes.explain()

from pyspark.sql.functions import max

flightData2015.select(max("count")).take(1)

from pyspark.sql.functions import desc

dfFlightData2015 = flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .sum("count")\
  .withColumnRenamed("sum(count)", "destination_total")\
  .sort(desc("destination_total"))\
  .limit(5)
  
dfFlightData2015.show()  
  
dfFlightData2015.explain()

##############################################
# Using SQL to process dataframe
##############################################

flightData2015.createOrReplaceTempView("flight_data_2015")

dfUsingSQL = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")

dfUsingSQL.explain()


maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

maxSql.show()

# Stop the sparksession
spark.stop()










