# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext

sc = SparkContext("local", "takeSample")

rdd = sc.parallelize(range(0, 10))

#countByKey : Count the number of elements for each key, and return the result as a dictionary.

rdd = sc.parallelize([("python", 12), ("pyspark", 11), ("spark", 11), ("hadoop", 11), ("python", 12), ("pyspark", 1), ("spark", 1)])
out1 = sorted(rdd.countByKey().items())
print(out1)


sc.stop()

