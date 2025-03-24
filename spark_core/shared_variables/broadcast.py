# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext

sc = SparkContext("local", "accumulators")

# Example 1
words_new = sc.broadcast(["scala", "java", "python", "spark", "hadoop"])   
data = words_new.value   
print("Stored data -> %s" % (data))   

elem = words_new.value[2]   
print("Printing a particular element in RDD : %s" % (elem))  

# Example 2
lookup = sc.broadcast({1: 'a', 2:'e', 3:'i', 4:'o', 5:'u'}) 
result = sc.parallelize([2, 1, 3]).map(lambda x: lookup.value[x]) 
print( result.collect() )

sc.stop()
