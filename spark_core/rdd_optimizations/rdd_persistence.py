# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext
import pyspark
 
data_path = "C:\\PySpark\\data"

sc = SparkContext("local", "persistence")

# Note: In Python, stored objects will always be serialized with the Pickle library, 
# so it does not matter whether you choose a serialized level.

rdd1 = sc.parallelize(range(1, 100), 5)
print( rdd1.getStorageLevel() )

rdd1.cache()
print( rdd1.getStorageLevel() )

rdd1.persist(pyspark.StorageLevel.MEMORY_ONLY)
print( rdd1.getStorageLevel() )

'''
rdd1.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
print( rdd1.getStorageLevel() )

rdd1.persist(pyspark.StorageLevel.DISK_ONLY)
print( rdd1.getStorageLevel() )

rdd1.persist(pyspark.StorageLevel.MEMORY_ONLY_SER)
print( rdd1.getStorageLevel() )

rdd1.persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
print( rdd1.getStorageLevel() )

rdd1.persist(pyspark.StorageLevel.MEMORY_ONLY_2)
print( rdd1.getStorageLevel() )

rdd1.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
print( rdd1.getStorageLevel() )
'''

rdd1.count()

rdd1.unpersist()

sc.stop()
