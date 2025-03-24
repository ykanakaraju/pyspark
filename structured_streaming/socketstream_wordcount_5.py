#!/usr/bin/env python2
# -*- coding: utf-8 -*-

##-------------------------------------------
# Using Window Operations  
##-------------------------------------------

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

if __name__ == "__main__":
    
    host = 'localhost'      # sys.argv[1]
    port = 9999             # int(sys.argv[2])
    
    spark = SparkSession\
        .builder\
        .appName("Socket Streaming")\
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")   
    spark.conf.set("spark.sql.shuffle.partitions", "1")

    # Create DataFrame representing the stream of input lines from connection to host:port
    lines = spark\
        .readStream\
        .format('socket')\
        .option('host', host)\
        .option('port', port)\
        .load()

    # Split the lines into words
    words = lines.select(explode(split(lines.value, ' ')).alias('word')) \
                 .withColumn("ts", current_timestamp())

    # Generate running word count
    # hopping window: window(words.ts, "6 seconds", "6 seconds")
    # sliding window: window(words.ts, "6 seconds", "3 seconds")
    wordCounts = words \
                 .withWatermark("ts", "3 seconds")\
                 .groupBy(
                      window(words.ts, "6 seconds", "6 seconds"),
                      words.word) \
                 .count()                    
    
    # Start running the query that prints the running counts to the console
    query = wordCounts \
        .writeStream \
        .outputMode('append') \
        .trigger(processingTime='3 seconds') \
        .format('console') \
        .start()

    query.awaitTermination()

