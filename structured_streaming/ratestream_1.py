#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from __future__ import print_function

import sys
import os

# setup the environment for the IDE
os.environ['SPARK_HOME'] = '/home/kanak/spark-2.4.7-bin-hadoop2.7'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python'

sys.path.append('/home/kanak/spark-2.4.7-bin-hadoop2.7/python')
sys.path.append('/home/kanak/spark-2.4.7-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip')

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    
    spark = SparkSession\
        .builder\
        .appName("Rate Streaming")\
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")    
       
    df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    # df will have two columns: timestamp, value

    resultDF = df.withColumn("newValue", df.value * 10)
     # columns: timestamp, value, newValue
    
    query = resultDF\
        .writeStream\
        .outputMode('append')\
        .option("truncate", False)\
        .format('console')\
        .start()
   

    query.awaitTermination()

