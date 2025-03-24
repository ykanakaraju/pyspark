# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext
from operator import add

sc = SparkContext("local", "map")

out1 = sc.parallelize([1, 2, 3, 4, 5]).reduce(add)
print(out1)

out2 = sc.parallelize((2 for _ in range(10))).map(lambda x: x*x).cache().reduce(add)
print(out2)

sc.stop()

