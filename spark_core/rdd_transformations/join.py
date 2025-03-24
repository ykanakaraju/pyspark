# -*- coding: utf-8 -*-
import findspark
findspark.init()
from pyspark import SparkContext

sc = SparkContext("local", "Joins")

names1 = sc.parallelize(["harsha", "aditya", "raju", "veer"]).map(lambda a: (a, 1))
names2 = sc.parallelize(["amrita", "raju", "veer", "anita"]).map(lambda a: (a, 1))

print( names1.collect() )
print( names2.collect() )

join = names1.join(names2)
print( join.collect() )

leftOuterJoin = names1.leftOuterJoin(names2)
print( leftOuterJoin.collect() )

rightOuterJoin = names1.rightOuterJoin(names2)
print( rightOuterJoin.collect() )

fullOuterJoin = names1.fullOuterJoin(names2)
print( fullOuterJoin.collect() )