# -*- coding: utf-8 -*-
'''
Problem Statement:
    Find the top 5 by count of starting letter of the name from Baby_Names dataset
    Show the starting letter of the name with count arranged in the descending order of count
'''

import findspark
findspark.init()

import os
from pyspark import SparkContext

sc = SparkContext("local", "sortGroup")

data_path = "C:\\PySpark\\data"

baby_names_csv_file = os.path.join(data_path, "Baby_Names.csv")

baby_names = sc.textFile(baby_names_csv_file) \
               .map(lambda line: line.split(",")) \
               .map(lambda arr: arr[1])  \
               .groupBy(lambda name: name[0]) \
               .map(lambda t: (t[0], len(t[1]))) \
               .sortBy(lambda a: a[1], ascending=False)


print( baby_names.take(5) )

