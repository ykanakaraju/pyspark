# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext
 
data_path = "C:\\PySpark\\data"

sc = SparkContext("local[2]", "repartition")

nums = range(1,30)

rdd1 = sc.parallelize(nums, 2)
print("rdd1 partitions: {}".format( rdd1.getNumPartitions() ) )

print("Number of partitions: {}".format(rdd1.getNumPartitions()))
print("Partitioner: {}".format(rdd1.partitioner))
print("Partitions structure: {}".format(rdd1.glom().collect()))

# partitionBy requires data to be in (k,v) pairs
# splitting data into 3 chunks using default hash partitioner
rdd2 = sc.parallelize(nums) \
        .map(lambda el: (el, el)) \
        .partitionBy(3) \
        .persist()
    
print("Number of partitions: {}".format(rdd2.getNumPartitions()))
print("Partitioner: {}".format(rdd2.partitioner))
print("Partitions structure: {}".format(rdd2.glom().collect()))

##################################
# Custom Partitioner Example
##################################
transactions = [
    {'name': 'Raju', 'amount': 100, 'city': 'Chennai'},
    {'name': 'Mahesh', 'amount': 15, 'city': 'Hyderabad'},
    {'name': 'Madhu', 'amount': 51, 'city': 'Hyderabad'},
    {'name': 'Revati', 'amount': 200, 'city': 'Chennai'},
    {'name': 'Amrita', 'amount': 75, 'city': 'Pune'},
    {'name': 'Aditya', 'amount': 175, 'city': 'Bangalore'},
    {'name': 'Keertana', 'amount': 105, 'city': 'Pune'},
    {'name': 'Keertana', 'amount': 105, 'city': 'Vijayawada'}
]

def city_partitioner(city):
    return hash(city)

# Validate results
num_partitions = 3

rdd3 = sc.parallelize(transactions) \
        .map(lambda e: (e['city'], e)) \
        .partitionBy(num_partitions, city_partitioner)
    
print("Number of partitions: {}".format(rdd3.getNumPartitions()))
print("Partitioner: {}".format(rdd3.partitioner))
print("Partitions structure: {}".format(rdd3.glom().collect()))
    
sc.stop()
