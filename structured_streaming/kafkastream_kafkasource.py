#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from __future__ import print_function

import sys
import os

# setup the environment for the IDE
os.environ['SPARK_HOME'] = '/home/kanak/spark-2.4.7-bin-hadoop2.7'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.7'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.7,org.apache.kafka:kafka-clients:2.8.1'
sys.path.append('/home/kanak/spark-2.4.7-bin-hadoop2.7/python')
sys.path.append('/home/kanak/spark-2.4.7-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip')

from pyspark.sql import SparkSession


if __name__ == "__main__":
    
    spark = SparkSession\
        .builder\
        .appName("Kafka Streaming")\
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")    

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "t1") \
        .load()

    output = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    query = output\
        .writeStream\
        .outputMode('append')\
        .format('console')\
        .start()

    query.awaitTermination()

