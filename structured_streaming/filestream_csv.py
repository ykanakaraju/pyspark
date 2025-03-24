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
from pyspark.sql.functions import explode, split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    
    spark = SparkSession\
        .builder\
        .appName("File Streaming")\
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")    

    path = "/home/kanak/sparkdir"
    
    mySchema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("deptid", IntegerType(), True)])    
    
    df = spark.readStream.csv(path, header=True, schema = mySchema)      
          
    groupDF = df.select("deptid").groupBy("deptid").count()
    
    query = groupDF\
        .writeStream\
        .outputMode('update')\
        .format('console')\
        .start()

    query.awaitTermination()

