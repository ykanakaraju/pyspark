# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession

flight_json_dir = "E:\\PySpark\\data\\flight-data\\json\\"
flight_json_2015 = flight_json_dir + "2015-summary.json"

spark = SparkSession \
    .builder \
    .appName("Basic Dataframe Operations") \
    .config("spark.master", "local") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", '10')

'''
spark.conf.set("spark.executor.memory", '8g')
spark.conf.set('spark.executor.cores', '3')
spark.conf.set('spark.cores.max', '3')
spark.conf.set("spark.driver.memory",'8g')
'''
   
######################################################    
#  Reading from JSON files
######################################################   

#df = spark.read.format("json").load(flight_json_2015)
df = spark.read.json(flight_json_2015)

# Schema inference
df.schema
df.printSchema()
df.columns   # lists all the columns
df.show(30, truncate=False)

######################################################  
# Basic Ops - Programmatic Schema
###################################################### 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

my_fields = [StructField('ORIGIN_COUNTRY_NAME', StringType(), True), 
             StructField('DEST_COUNTRY_NAME', StringType(), True),
             StructField('count', IntegerType(), True)]

my_schema  = StructType(my_fields)

df_custom_schema = spark.read.schema(my_schema).json(flight_json_2015)

df_custom_schema.printSchema()

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

df.select(expr("DEST_COUNTRY_NAME AS destination"), col("ORIGIN_COUNTRY_NAME").alias("origin") ).show(2)

df.select(expr("DEST_COUNTRY_NAME").alias("destination"))\
.show(2)


# selectExpr
df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)
#df.select(expr("DEST_COUNTRY_NAME as newColumnName"), expr("DEST_COUNTRY_NAME")).show(2)

df.selectExpr(
  "*", # all original columns
  "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry") \
  .show(2)

df.selectExpr("avg(count) as AvgCount", "count(distinct(DEST_COUNTRY_NAME)) as DistCountries").show(2)

######################################################  
# Basic Ops - lit
###################################################### 
from pyspark.sql.functions import lit
df.select(expr("*"), lit(1).alias("One")).show(2)

df.select("*", lit("Hello").alias("Hi")).show(2)

######################################################  
# Basic Ops - withColumn (Adding a new column)
###################################################### 

df.show(2)

df.withColumn("numberOne", lit(1)).show(2)
df.withColumn("numberOne", lit(1)).printSchema()

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME = DEST_COUNTRY_NAME"))\
  .show(2)

# one way to rename a column without using withColumnRenamed method  
df.withColumn("Destination", expr("DEST_COUNTRY_NAME")).columns
  
######################################################  
# Basic Ops - withColumnRenamed (Renaming column)
###################################################### 

df.withColumnRenamed("DEST_COUNTRY_NAME", "destination") \
  .withColumnRenamed("ORIGIN_COUNTRY_NAME", "origin") \
  .show(5)

df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns

######################################################  
# Basic Ops - drop (Drop columns)
###################################################### 
df.printSchema()

df2 = df.drop("ORIGIN_COUNTRY_NAME")
df2.printSchema()

df.printSchema()
df.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").show(5)

######################################################  
# Basic Ops - cast (Casting from one type to another)
###################################################### 
df.printSchema()
df.withColumn("count2", col("count").cast("int")).printSchema()

df.select("DEST_COUNTRY_NAME",  "ORIGIN_COUNTRY_NAME", col("count").cast("int")).printSchema()

'''
df.createOrReplaceTempView("flights")
qry = "select DEST_COUNTRY_NAME as dest, ORIGIN_COUNTRY_NAME as origin, count from flights"
spark.sql(qry).printSchema()
df.printSchema()
'''

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

# df.createOrReplaceTempView("flights")  
# qry = "select COUNT( DISTINCT(ORIGIN_COUNTRY_NAME) )  as distinct_origin from flights"
# spark.sql(qry).show()
  
df.select("ORIGIN_COUNTRY_NAME").distinct().count()

df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()

######################################################  
# Random Sampling - sample
######################################################
seed = 25
withReplacement = False
fraction = 0.05

df.sample(withReplacement, fraction, seed).show()

######################################################  
# Random Splits - randomSplit
######################################################
seed = 5
randomDfs = df.randomSplit([0.25, 0.35, 0.4], 5)
randomDfs[0].count() 
randomDfs[1].count() 
randomDfs[2].count() 

######################################################  
# Concatinate Dataframes - union
######################################################

from pyspark.sql import Row
schema = df.schema
row1 = Row("India", "UK", 234)
row2 = Row("India", "India", 234)
newDF = spark.createDataFrame([row1, row2], schema)

df.show()
newDF.show()
newDF.printSchema()

df.union(newDF)\
.where("DEST_COUNTRY_NAME = 'India'")\
.where(col("ORIGIN_COUNTRY_NAME") != "United States")\
.show()

######################################################  
# Sorting
######################################################
from pyspark.sql.functions import desc, asc, expr

df.sort("count").show(5)
df.sort( desc("count") ).show(10)
df.sort( col("count").desc() ).show(10)

df.orderBy("DEST_COUNTRY_NAME", "count").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

df.orderBy(col("ORIGIN_COUNTRY_NAME"), desc("count")).show(100)

######################################################  
# limit
######################################################

df.limit(5).show()

df.show(5)

df.orderBy(expr("count desc")).limit(6).show()

######################################################  
# repartition & coalesce
######################################################

df.rdd.getNumPartitions()  # 1 partition

df.show(10)
df4 = df.repartition(col("DEST_COUNTRY_NAME"))
df4.rdd.getNumPartitions() 

df2 = df.repartition(4, col("DEST_COUNTRY_NAME"))
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
collectDF.show(5)

collectDF.show() 
collectDF.collect()

collectDF.schema
collectDF.printSchema()


# Stop the sparksession
spark.stop()