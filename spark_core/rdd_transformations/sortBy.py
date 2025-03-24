# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext

sc = SparkContext("local", "sortBy")

#############  Example 1 ################
names = sc.parallelize(["Ramya", "Rohit", "Aditya", , "Ramesh"
                    "Chetan", "Charlee", "Bhagat", "Arjun",
                    "Bharat", "Amit", "Bhavya", "Karan"])

sorted_1 = names.sortBy(lambda a: a)
print( sorted_1.collect() )

sorted_2 = names.sortBy(lambda a: a, ascending=False)
print( sorted_2.collect() )

sorted_3 = names.sortBy(lambda a: len(a))
print( sorted_3.collect() )

sorted_4 = names.sortBy(lambda a: a).sortBy(lambda a: len(a))
print( sorted_4.collect() )

sc.stop()
