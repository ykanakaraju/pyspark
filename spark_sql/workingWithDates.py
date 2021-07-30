# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import*
from pyspark.sql.window import *

spark = SparkSession \
    .builder \
    .appName("Basic Dataframe Operations") \
    .config("spark.master", "local") \
    .getOrCreate()
    
users = spark.createDataFrame([
    (1, "Raju", "05-01-1975"),
    (2, "Ramesh", "07-02-1991"),
    (3, "Amrita", "10-03-1995"),
    (4, "Madhu", "03-04-1997"),
    (5, "Aditya", "02-05-2000"),
    (6, "Pranav", "23-06-1985")])\
  .toDF("id", "name", "dob")
  
users.printSchema()
users.show()  

df2 = users.withColumn("date", to_date(col("dob"), "d-MM-yyyy")) \
        .withColumn("current_timestamp", current_timestamp()) \
        .withColumn("today", current_date()) \
        .withColumn("tomorrow", date_add("current_date", 1)) \
        .withColumn("yesterday", date_sub("current_date", 1))
  
df2.show(10, False)
df2.printSchema()
df2.schema

df3 = users.withColumn("date", to_date(col("dob"), "d-MM-yyyy")) \
        .withColumn("new_date", date_format("date", "dd/MM/yyyy")) \
        .withColumn("year_trunc", date_trunc("year", "date")) \
        .withColumn("month_trunc", date_trunc("month", "date"))

df3.show(10, False)
df3.printSchema()


df4 = users.withColumn("date", to_date(col("dob"), "d-MM-yyyy")) \
        .withColumn("today", current_date()) \
        .withColumn("diff", datediff("today", "date")) \
        .withColumn("month_day", dayofmonth("date")) \
        .withColumn("week_day", dayofweek("date"))
        
df4.show(10, False)
df4.printSchema()        


# Ref link: https://medium.com/expedia-group-tech/deep-dive-into-apache-spark-datetime-functions-b66de737950a








