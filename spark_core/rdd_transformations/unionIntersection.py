# -*- coding: utf-8 -*-
import findspark
findspark.init()
from pyspark import SparkContext

sc = SparkContext("local", "Union")

rdd1 = sc.parallelize([1,2,3,4,5,6,7,8,9])
rdd2 = sc.parallelize([1,2,'a','b','c'])
#rdd2 = sc.parallelize([5,6,7,8,9,10,11,12,13,14])

# Union
union = rdd1.union(rdd2).collect()
print(union) 

#intersection
intersection = rdd1.intersection(rdd2).collect()
print(intersection) 