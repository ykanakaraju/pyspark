# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext

data_path = "C:\\PySpark\\data"

sc = SparkContext("local", "combineByKey")


x = sc.parallelize([("a", 1), ("b", 1), ("a", 2)])

def to_list(a):
	return [a]

def append(a, b):
    a.append(b)
    return a

def extend(a, b):
    a.extend(b)
    return a

out = sorted(x.combineByKey(to_list, append, extend).collect())

print(out)

sc.stop()

