# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
import pandas as pd 

data_path = "C:\\PySpark\\data\\"

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Basics") \
    .config("spark.master", "local") \
    .getOrCreate()
    
pd_df = pd.read_csv("C:\\PySpark\\data\\addresses.csv")

schema = StructType( [StructField("name", StringType(), True),
                      StructField("address", StringType(), True),
                      StructField("city", StringType(), True),
                      StructField("state", StringType(), True),
                      StructField("zip", IntegerType(), True)])

sp_df = spark.createDataFrame(pd_df, schema=schema)
sp_df.printSchema()

sp_df.createGlobalTempView("addresses")
addresses_df = spark.sql("SELECT * FROM global_temp.addresses")
addresses_df.show()

count = addresses_df

spark.stop()