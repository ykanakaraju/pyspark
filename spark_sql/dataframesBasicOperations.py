# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession

flight_json_dir = "C:\\PySpark\\data\\flight-data\\json\\"
flight_json_2015 = flight_json_dir + "2015-summary.json"

spark = SparkSession \
    .builder \
    .appName("Basic Dataframe Operations") \
    .config("spark.master", "local") \
    .getOrCreate()

'''
spark.conf.set("spark.executor.memory", '8g')
spark.conf.set('spark.executor.cores', '3')
spark.conf.set('spark.cores.max', '3')
spark.conf.set("spark.driver.memory",'8g')
'''
   
######################################################    
#  Reading from JSON files
######################################################   

df = spark.read.format("json").load(flight_json_2015)

# Schema inference
df.schema
df.printSchema()
df.columns   # lists all the columns
df.show(30, truncate=False)

######################################################  
# Basic Ops - select & selectExpr
######################################################  

df.select("DEST_COUNTRY_NAME").show(2)

df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

from pyspark.sql.functions import expr, col, column

df.select(expr("DEST_COUNTRY_NAME"), 
          col("DEST_COUNTRY_NAME"), 
          column("DEST_COUNTRY_NAME")) \
  .show(2)

df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME"))\
.show(2)

# selectExpr
df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)

df.selectExpr(
  "*", # all original columns
  "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")\
  .show(2)

df.selectExpr("avg(count) as AvgCount", "count(distinct(DEST_COUNTRY_NAME)) as DistCountries").show(2)

######################################################  
# Basic Ops - lit
###################################################### 
from pyspark.sql.functions import lit
df.select(expr("*"), lit(1).alias("One")).show(2)

######################################################  
# Basic Ops - withColumn (Adding a new column)
###################################################### 

df.show(2)

df.withColumn("numberOne", lit("Hi")).show(2)

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))\
  .show(2)

# one way to rename a column without using withColumnRenamed method  
df.withColumn("Destination", expr("DEST_COUNTRY_NAME")).columns
  
######################################################  
# Basic Ops - withColumnRenamed (Renaming column)
###################################################### 

df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns

######################################################  
# Basic Ops - drop (Drop columns)
###################################################### 
df.drop("ORIGIN_COUNTRY_NAME").columns
df.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")

######################################################  
# Basic Ops - cast (Casting from one type to another)
###################################################### 
df.withColumn("count2", col("count").cast("long")).printSchema()

######################################################  
# Filtering Data - filter & where 
######################################################
df.filter(col("count") < 2).show(2)
df.filter("count < 2").show(2)

df.where(col("count") < 2).show(2)
df.where("count < 2").show(2)

df.where(col("count") < 2) \
  .where(col("ORIGIN_COUNTRY_NAME") != "Croatia") \
  .show(2)

######################################################  
# Fetch unique records - distinct
######################################################
df.select("ORIGIN_COUNTRY_NAME").distinct().count()

df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()

######################################################  
# Random Sampling - sample
######################################################
seed = 5
withReplacement = False
fraction = 0.03
df.sample(withReplacement, fraction, seed).show()

######################################################  
# Random Splits - randomSplit
######################################################
seed = 5
randomDfs = df.randomSplit([0.25, 0.35, 0.4], seed)
randomDfs[0].count() 
randomDfs[1].count() 
randomDfs[2].count() 

######################################################  
# Concatinate Dataframes - union
######################################################

from pyspark.sql import Row
schema = df.schema
row1 = Row("India", "UK", 1)
row2 = Row("UK", "India", 1)
newDF = spark.createDataFrame([row1, row2], schema)

df.show()
newDF.show()

df.union(newDF)\
.where("count = 1")\
.where(col("ORIGIN_COUNTRY_NAME") != "United States")\
.show()

######################################################  
# Sorting
######################################################

df.sort("count").show(5)
df.orderBy("DEST_COUNTRY_NAME", "count").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

from pyspark.sql.functions import desc, asc
df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)

######################################################  
# limit
######################################################

df.limit(5).show()
df.orderBy(expr("count desc")).limit(6).show()


######################################################  
# repartition & coalesce
######################################################

df.rdd.getNumPartitions()  # 1 partition

df.repartition(col("DEST_COUNTRY_NAME"))
df.rdd.getNumPartitions() 

df2 = df.repartition(5, col("DEST_COUNTRY_NAME"))
df2.rdd.getNumPartitions()

df3 = df2.coalesce(2)
df3.rdd.getNumPartitions()

df.rdd.getNumPartitions() 
df.repartition(col("DEST_COUNTRY_NAME"))
df.repartition(5, col("DEST_COUNTRY_NAME"))
df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)

######################################################  
# collect data to the driver
######################################################
collectDF = df.limit(10)
collectDF.take(5) 
collectDF.show() 
collectDF.show(5)
collectDF.collect()

collectDF.schema
collectDF.printSchema()


# Stop the sparksession
spark.stop()