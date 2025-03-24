#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from __future__ import print_function

'''
IMPORTANT NOTE:
    In this example we are writing data to two different sinks by executing two different
    streaming queries. That means, the data is being read twice and separatly for both the
    sinks. 
    
    Do not mistake that we are reading the data once and writing to two sinks. That is NOT
    the case here.
'''

import sys
import os

# setup the environment for the IDE
os.environ['SPARK_HOME'] = '/home/kanak/spark-2.4.7-bin-hadoop2.7'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python'

sys.path.append('/home/kanak/spark-2.4.7-bin-hadoop2.7/python')
sys.path.append('/home/kanak/spark-2.4.7-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat
#from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    
    spark = SparkSession\
        .builder\
        .appName("Rate Streaming")\
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")  
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/home/kanak/pyspark/checkpoint/file_sink");
       
    df = spark.readStream.format("rate").option("rowsPerSecond", 2).load()
    # df will have two columns: timestamp, value

    resultDF = df.withColumn("value", concat(lit("Message: "), col("value"))) \
                 .selectExpr("CAST(timestamp AS STRING)", "CAST(value AS STRING)") \
                 .withColumnRenamed("timestamp", "key")
    
    # write to csv file sink
    query = resultDF.writeStream \
        .outputMode('append') \
        .trigger(processingTime='5 seconds') \
        .format('csv') \
        .option("path", "/home/kanak/pyspark/output/csv") \
        .start()   
        
    # write to kafka sink
    query2 = resultDF.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "t1") \
        .start()    

    query.awaitTermination()
    query2.awaitTermination()

