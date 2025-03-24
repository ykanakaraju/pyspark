# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext

sc = SparkContext("local", "takeSample")

rdd = sc.parallelize(range(0, 10))

rdd.count()
# takeSample(withReplacement, num, seed=None)

sample1 = rdd.takeSample(True, 20, 1)
print(sample1)

sample2 = rdd.takeSample(False, 5, 4)
print(sample2)

sample3 = rdd.takeSample(False, 15, 3)
print(sample3)

sc.stop()

