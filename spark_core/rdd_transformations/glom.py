# -*- coding: utf-8 -*-
import findspark
findspark.init()
from pyspark import SparkContext

sc = SparkContext("local", "subtractByKey")

dataList = [10,4,59,6,20,54,23,4,1,56,39,23,12,34,1,2,4,3,21,32,33]
dataRDD = sc.parallelize(dataList, 3)
print( dataRDD.collect() )
print( "Partition Count: ", dataRDD.getNumPartitions() )

print( dataRDD.glom().collect() )