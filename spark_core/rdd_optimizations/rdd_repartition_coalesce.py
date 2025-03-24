# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext
 
data_path = "C:\\PySpark\\data"

sc = SparkContext("local", "repartition")

rdd1 = sc.parallelize(range(1,100), 2)
print("rdd1 partitions: {}".format( rdd1.getNumPartitions() ) )

rdd2 = rdd1.repartition(5)
print("rdd2 partitions: {}".format( rdd2.getNumPartitions() ) )

rdd3 = rdd2.coalesce(3)
print("rdd3 partitions: {}".format( rdd3.getNumPartitions() ) )

sc.stop()
