# -*- coding: utf-8 -*-
import findspark
findspark.init()
from pyspark import SparkContext

sc = SparkContext("local", "Union")

rdd1 = sc.parallelize([("key1", 1), ("key2", 2), ("key1", 3)])       
rdd2 = sc.parallelize([("key1", 5), ("key2", 4), ("key2", 7)])

grp = rdd1.cogroup(rdd2).mapValues(lambda val: [i for e in val for i in e])
print(grp.collect())

sc.stop()