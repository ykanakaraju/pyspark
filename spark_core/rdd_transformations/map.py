# -*- coding: utf-8 -*-
import findspark
findspark.init()

import os
from pyspark import SparkContext

data_path = "C:\\PySpark\\data"

sc = SparkContext("local", "map")

#########################################
#   map - Example 1
#########################################
rdd_data = sc.parallelize([1,2,3]) 

rdd_map_1 = rdd_data.map(lambda x: [x,x,x])
for x in rdd_map_1.collect(): print(x) 

rdd_map_2 = rdd_data.map(lambda x: x**2)
for x in rdd_map_2.collect(): print(x) 

#########################################
#   map - Example 2
#########################################
baby_names_csv_file = os.path.join(data_path, "Baby_Names.csv")
baby_names = sc.textFile(baby_names_csv_file)
rows = baby_names.map(lambda line: line.split(","))
print("Total number of Rows:", rows.count())
for row in rows.take(10): print(row[1].lower())

# get names and their length
rows2 = rows.map(lambda line: (line[1], len(line[1])))
for x in rows2.take(10): print(x)

sc.stop()


