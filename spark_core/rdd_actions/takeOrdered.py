# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext

sc = SparkContext("local", "map")

out1 = sc.parallelize([10, 1, 2, 9, 3, 4, 5, 6, 7]).takeOrdered(6)
print(out1)

out2 = sc.parallelize([10, 1, 2, 9, 3, 4, 5, 6, 7], 2).takeOrdered(6, key=lambda x: -x)
print(out2)

sc.stop()

