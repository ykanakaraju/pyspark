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
from pyspark.sql.functions import explode, split, current_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    
    spark = SparkSession\
        .builder\
        .appName("Socket Streaming")\
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")  
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/home/kanak/pyspark/checkpoint/file_sink")

    # Create DataFrame representing the stream of input lines from connection to host:port
    path = "/home/kanak/sparkdir"
    
    mySchema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("deptid", IntegerType(), True)])    
    
    df = spark.readStream.csv(path, header=True, schema = mySchema)               
    
    # Start running the query that prints the running counts to the console
    query = df \
        .writeStream \
        .outputMode('append') \
        .trigger(processingTime='1 seconds') \
        .format('json') \
        .option("path", "/home/kanak/pyspark/output/json") \
        .start()

    
    query.awaitTermination()

