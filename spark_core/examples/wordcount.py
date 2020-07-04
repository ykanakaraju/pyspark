# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
import os
import shutil 

data_path = "C:\\PySpark\\data"

config = SparkConf().setAll([('spark.executor.memory', '4g'), ('spark.executor.cores', '2'), ('spark.cores.max', '2'), ('spark.driver.memory','4g')])  \
                    .setAppName('Wordcount')

sc = SparkContext(conf=config)

#sc = SparkContext("local", "wordcount")
       
text_file = sc.textFile(os.path.join(data_path, "wordcount.txt") )
        
wordcount = text_file.flatMap(lambda line: line.split(" "))  \
                .map(lambda word: (word, 1)) \
                .reduceByKey(lambda a, b: a + b)
    
output_path = os.path.join(data_path, "wordcount_out")

# delete the output directory if exists..
if (os.path.isdir(output_path)):
    shutil.rmtree(output_path)

wordcount.saveAsTextFile(output_path)

sc.stop()
