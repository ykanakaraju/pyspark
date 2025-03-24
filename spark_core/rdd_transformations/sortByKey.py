# -*- coding: utf-8 -*-
import findspark
findspark.init()
import os
from pyspark import SparkContext

data_path = "F:\\PySpark\\data"

sc = SparkContext("local", "sortByKey")

baby_names_csv_file = os.path.join(data_path, "Baby_Names.csv")
baby_names = sc.textFile(baby_names_csv_file)

rdd1 = baby_names.map(lambda line: line.split(",")) \
                 .map (lambda n: (str(n[1]), int(n[4]) ) ) 
print (rdd1.take(5))

sort_asc = rdd1.sortByKey()                 
print (sort_asc.take(5))

sort_desc = rdd1.sortByKey(ascending = False)                
print (sort_desc.take(5))
