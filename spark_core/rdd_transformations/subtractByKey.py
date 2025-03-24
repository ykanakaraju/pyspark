# -*- coding: utf-8 -*-
import findspark
findspark.init()
from pyspark import SparkContext

sc = SparkContext("local", "subtractByKey")

rdd1 = sc.parallelize([1,2,3,4,5,6,7,8,9])
rdd2 = sc.parallelize([5,6,7,8,9,10,11,12,13,14])

# subtract
rdd1 = sc.parallelize([(1, 2), (3, 4), (3, 6), (4, 4), (4, 6), (5, 6)])
rdd2 = sc.parallelize([(3, 4), (5, 2)])
    
sbkRdd = rdd1.subtractByKey(rdd2)
print(sbkRdd.collect())