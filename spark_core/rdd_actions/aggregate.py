# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext

sc = SparkContext("local", "aggregate")

seqOp = (lambda x, y: (x[0] + y, x[1] + 1))
combOp = (lambda x, y: (x[0] + y[0], x[1] + y[1]))

'''
Aggregate the elements of each partition, and then the results for all the partitions, 
using a given combine functions and a neutral “zero value.”
'''
rdd1 = sc.parallelize([1,2,3,4,5,6,7,8,9,10], 2)
sumCount = rdd1.aggregate((0, 0), seqOp, combOp)
print(sumCount)

sc.stop()