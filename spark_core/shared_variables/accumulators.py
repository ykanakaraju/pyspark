# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext

sc = SparkContext("local", "accumulators")

# Closure problem
counter = 0
def increment_counter(x):
    global counter
    counter += x

sc.parallelize(range(1, 10), 3).foreach(increment_counter)
print("counter: {}".format(counter))

# Accumulator Example 1
accum = sc.accumulator(0)
sc.parallelize(range(1, 10), 3).foreach(lambda x: accum.add(x))
print("accum.value: {}".format(accum.value))

#Accumulator Example 2
count = sc.accumulator(0) 

def add_accumulator(i):
    global count
    count.add(1)
    return i

result = sc.parallelize([1,2,3,4,5,6], 2) \
           .map(lambda i : add_accumulator(i)) \
           .reduce(lambda x,y: x+y)

print("result: {}".format(result))
print("count: {}".format(count))

sc.stop()
