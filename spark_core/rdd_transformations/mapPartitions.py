# -*- coding: utf-8 -*-
import findspark
findspark.init()
from pyspark import SparkContext

sc = SparkContext("local", "mapPartitons")

#########################################
#   mapPartitions - Example 1
#########################################
rdd_parallal = sc.parallelize(range(1, 12), 3)  #(1,2,3), (4,5,6,7), (8,9,10,11)

rdd_parallal.glom().collect()

print(list(range(1, 12)))

def total(iterator): 
    yield sum(iterator)

rdd_mp = rdd_parallal.mapPartitions(total)
print(rdd_mp.collect())

#########################################
#   mapPartitions - Example 2
#########################################
def show(iterator): 
    yield list(iterator)

rdd_mp2 = rdd_parallal.mapPartitions(show)
print(rdd_mp2.collect())

# example 3
rdd_parallal_2 = sc.parallelize(range(1, 12))
rdd_parallal_2.getNumPartitions()

rdd_mp_2 = rdd_parallal_2.mapPartitions(total)
print(rdd_mp_2.collect())

# What is the default parallalism on this system?
print("Default number of partitions: ", sc.defaultParallelism)

