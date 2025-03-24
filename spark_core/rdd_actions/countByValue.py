# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext

sc = SparkContext("local", "takeSample")

rdd = sc.parallelize(range(0, 10))

#countByValue : Return the count of each unique value in this RDD as a dictionary of (value, count) pairs.

rdd = sc.parallelize([("python", 1), ("pyspark", 1), ("spark", 1), ("hadoop", 1), ("python", 1), ("pyspark", 1), ("spark", 1)])
out1 = sorted(rdd.countByValue().items())
print(out1)

rdd2 = sc.parallelize([1,2,2,2,3,4,2,4,5,3,6,7,8,4,6,3])
out2 = sorted(rdd2.countByValue().items())
print(out2)

sc.stop()

