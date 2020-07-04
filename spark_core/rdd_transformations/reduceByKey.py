# -*- coding: utf-8 -*-
import findspark
findspark.init()
import os
from pyspark import SparkContext

data_path = "C:\\PySpark\\data"

sc = SparkContext("local", "sortByKey")

########### Example 1 ############
rdd1 = sc.parallelize([("USA", 1), ("USA", 2), ("India", 1),
                       ("UK", 1), ("India", 4), ("India", 9),
                       ("USA", 8), ("USA", 3), ("India", 4),
                       ("UK", 6), ("UK", 9), ("UK", 5)])

rdd2 = rdd1.reduceByKey( lambda a1, a2: a1 + a2 )
print ( rdd2.collect() )

########### Example 2 ############
baby_names_csv_file = os.path.join(data_path, "Baby_Names.csv")
baby_names = sc.textFile(baby_names_csv_file)

filtered_rows = baby_names.filter(lambda line: "Count" not in line)  \
                          .map(lambda line: line.split(",")) \
                          .map(lambda n:  (str(n[1]), int(n[4])))  \
                          .reduceByKey(lambda v1,v2: v1 + v2)
                          
print ( filtered_rows.collect() )