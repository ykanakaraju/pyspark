# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import Row

data_path = "C:\\PySpark\\data\\"

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Basics") \
    .config("spark.master", "local") \
    .getOrCreate()
  
# $example on:schema_inferring$
sc = spark.sparkContext

# Load a text file and convert each line to a Row.
lines = sc.textFile(data_path + "users.txt")
parts = lines.map(lambda l: l.split(","))
users = parts.map(lambda p: Row(id=int(p[0]), name=p[1], gender=p[2], age=int(p[3]), phone=p[4]))

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(users)
schemaPeople.printSchema()

schemaPeople.createOrReplaceTempView("users")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM users WHERE age >= 13 AND age <= 19")
teenagers.show()

# The results of SQL queries are Dataframe objects.
# rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
for name in teenNames:
    print(name)
    
spark.stop()