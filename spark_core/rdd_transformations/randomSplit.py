# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext

data_path = "C:\\PySpark\\data"

sc = SparkContext("local", "randomSplit")

#########################################
#   randomSplit - Example 1
#########################################
rdd_data = sc.parallelize(range(1,50)) 
rdd_splits = rdd_data.randomSplit([0.4, 0.3, 0.3])
rdd1 = rdd_splits[0]
rdd2 = rdd_splits[1]
rdd3 = rdd_splits[2]

rdd1.collect()
rdd2.collect()
rdd3.collect()


sc.stop()