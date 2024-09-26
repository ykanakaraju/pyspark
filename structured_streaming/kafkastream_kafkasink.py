#!/usr/bin/env python2
# -*- coding: utf-8 -*-

#bin/spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 /home/kanak/pyspark/structured_streaming/dev1.py

from __future__ import print_function
import sys, os

os.environ['SPARK_HOME'] = '/home/kanak/spark-2.4.7-bin-hadoop2.7'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python'
sys.path.append('/home/kanak/spark-2.4.7-bin-hadoop2.7/python')
sys.path.append('/home/kanak/spark-2.4.7-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat

if __name__ == "__main__":
    
    spark = SparkSession\
        .builder\
        .appName("Rate Streaming")\
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR") 
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/home/kanak/pyspark/kafka_checkpoint");
       
    df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()
    # timestamp, value  1  -> "Message: 1"

    resultDF = df.withColumn("value", concat(lit("Message: "), col("value"))) \
                 .selectExpr("CAST(timestamp AS STRING)", "CAST(value AS STRING)") \
                 .withColumnRenamed("timestamp", "key")                 
    # key: timestamp, value: Message: 1
    
    query = resultDF.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "t1") \
        .start()

    query.awaitTermination()

