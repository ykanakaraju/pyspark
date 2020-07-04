# -*- coding: utf-8 -*-
import findspark
findspark.init()

import os
from pyspark import SparkContext, SparkConf

data_path = "C:\\PySpark\\data"

config = SparkConf().setAll([('spark.executor.memory', '4g'), ('spark.executor.cores', '2'), ('spark.cores.max', '2'), ('spark.driver.memory','4g')])

sc = SparkContext(conf=config)

baby_names_csv_file = os.path.join(data_path, "Baby_Names.csv")
baby_names = sc.textFile(baby_names_csv_file)

jacksons = baby_names.filter(lambda line: "JACKSON" in line)

print("Number of with JACKSON: ", jacksons.count())
#print(jacksons.collect())
for x in jacksons.collect():
    print(x)
    
sc.stop()
