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
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == "__main__":
    
    path = "/home/kanak/sparkdir"
    
    spark = SparkSession\
        .builder\
        .appName("File Streaming")\
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")    

    lines = spark\
        .readStream\
        .format('text')\
        .option("path", path)\
        .option("maxFilesPerTrigger", 1)\
        .load()

    # Split the lines into words
    words = lines.select(
        explode(
            split(lines.value, ' ')
        ).alias('word')
    )

    # Generate running word count
    wordCounts = words.groupBy('word').count()

    # Start running the query that prints the running counts to the console
    query = wordCounts\
        .writeStream\
        .outputMode('update')\
        .format('console')\
        .start()

    query.awaitTermination()

