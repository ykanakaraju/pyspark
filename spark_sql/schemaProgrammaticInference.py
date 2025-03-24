# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql import Row
from pyspark.sql.types import *

data_path = "C:\\PySpark\\data\\"

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Basics") \
    .config("spark.master", "local") \
    .getOrCreate()
  
sc = spark.sparkContext

lines = sc.textFile(data_path + "people.txt")
parts = lines.map(lambda l: l.split(","))

people = parts.map(lambda p: (p[0], p[1].strip()))

schemaString = "name age"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD.
schemaPeople = spark.createDataFrame(people, schema)

# Creates a temporary view using the DataFrame
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
results = spark.sql("SELECT name FROM people")

results.show()

    
spark.stop()