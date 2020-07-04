# -*- coding: utf-8 -*-
import findspark
findspark.init()
from pyspark import SparkContext

sc = SparkContext("local", "flatmap")

#########################################
#   flatMap Vs map - Example 1
#########################################
rdd_data = sc.parallelize([1,2,3]) 

rdd_flatmap = rdd_data.flatMap(lambda x: [x,x,x])
#for x in rdd_flatmap_1.collect(): print(x)
print(rdd_flatmap.collect())

rdd_map = rdd_data.map(lambda x: [x,x,x])
print(rdd_map.collect())

