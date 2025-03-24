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
       
    df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    # df will have two columns: timestamp, value
    
    resultDF = df.withColumn("value", concat(lit("Message: "), col("value"))) \
                 .selectExpr("CAST(timestamp AS STRING)", "CAST(value AS STRING)") \
                 .withColumnRenamed("timestamp", "ts")\
                 .withColumnRenamed("value", "message")                  
    # resultDF columns:  ts, message
    
    def process_data(df, epoch_id):
        
        # write to mysql
        df.write.format("jdbc") \
            .option("url", "jdbc:mysql://localhost/sparkdb?autoReConnect=true&useSSL=false") \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("dbtable", "rate_stream_1")  \
            .option("user", "root") \
            .option("password", "kanakaraju") \
            .mode("append") \
            .save()            
          
        # write to csv files
        df.write.csv("/home/kanak/pyspark/output/csv/csv_sink_1", mode="append")  
        
        # write to Kafka
        df.withColumnRenamed("ts", "key")\
            .withColumnRenamed("message", "value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "t1") \
            .save()
 
        pass
    
    query = resultDF.writeStream.foreachBatch(process_data).start()
    
    query.awaitTermination()

    
