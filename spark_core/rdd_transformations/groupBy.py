# -*- coding: utf-8 -*-
import findspark
findspark.init()

import os
from pyspark import SparkContext

data_path = "F:\\PySpark\\data"

sc = SparkContext("local", "groupby")

#############  Example 1 ################
x = sc.parallelize(["Ramya", "Ramesh", "Rajeev", 
                    "Chetan", "Charlee", "Bhagat",
                    "Bharat", "Bony", "Bhavya"], 3)
 
# Applying groupBy operation on x
y = x.groupBy(lambda word: word[0])
 
for t in y.collect():
    print((t[0],[i for i in t[1]]))
    
#############  Example 2 ################
baby_names_csv_file = os.path.join(data_path, "Baby_Names.csv")
baby_names = sc.textFile(baby_names_csv_file)

rdd1 = baby_names.map(lambda line: line.split(",")) 
rdd2 = rdd1.map(lambda arr: arr[1])  
#print( rdd2.take(5) )
rdd3 = rdd2.groupBy(lambda name: name[0])

#for t in rdd3.take(5): print((t[0],[i for i in t[1]]))

rdd4 = rdd3.map(lambda t: (t[0], len(t[1])))
print( rdd4.take(5) )