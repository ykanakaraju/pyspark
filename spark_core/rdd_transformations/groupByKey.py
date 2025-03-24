# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext

data_path = "C:\\PySpark\\data"

sc = SparkContext("local", "map")

rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1), ("a", 4), 
                      ("b", 5), ("c", 1), ("c", 4), ("c", 5), ("a", 1)])

out1 = sorted(rdd.groupByKey().mapValues(len).collect())
print(out1)

out2 = sorted(rdd.groupByKey().mapValues(list).collect())
print(out2)

sc.stop()


#rdd.groupByKey() =>  

