# -*- coding: utf-8 -*-
import findspark
findspark.init()
from pyspark import SparkContext

sc = SparkContext("local", "mapPartitonsWithIndex")

#########################################
#   mapPartitions - Example 1
#########################################
rdd_parallal = sc.parallelize(range(1, 12), 3)  #(1,2,3,4), (5,6,7,8), (9,10,11,12)

def show(index, iterator): 
    yield 'index: ' + str(index) + "; values: " + str(list(iterator))

rdd_mpwi = rdd_parallal.mapPartitionsWithIndex(show)
print(rdd_mpwi.collect())


#########################################
#   mapPartitions - Example 2
#########################################
rdd_parallal_2 = sc.parallelize(["white", "magenta", "red", "green", "blue", "cyan", "black"], 3);
    
def show_2(index, iterator): 
    out = ""
    iter1 = map(lambda x: x + " (" + str(len(x)) + ")" , iterator)
    
    for e in list(iter1):
        if out == "":
            out = e
        else:
            out += ", " + e  
          
    yield '\nCalled in Partition: ' + str(index) + "; \nValues: " + out
    
rdd_mpwi_2 = rdd_parallal_2.mapPartitionsWithIndex(show_2)
 
for x in  rdd_mpwi_2.collect(): 
    print(x)
    
        
sc.stop()
    
    
    
    