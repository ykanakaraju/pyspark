# -*- coding: utf-8 -*-
import findspark
findspark.init()
from pyspark import SparkContext

data_path = "C:\\PySpark\\data"

sc = SparkContext("local", "aggregateByKey")

# Creating PairRDD student_rdd with key value pairs
student_rdd = sc.parallelize([
  ("Aditya", "Maths", 83), ("Aditya", "Physics", 74), ("Aditya", "Chemistry", 91), ("Aditya", "English", 82), 
  ("Amrita", "Maths", 69), ("Amrita", "Physics", 62), ("Amrita", "Chemistry", 97), ("Amrita", "English", 80), 
  ("Pranav", "Maths", 78), ("Pranav", "Physics", 73), ("Pranav", "Chemistry", 68), ("Pranav", "English", 87), 
  ("Keerthana", "Maths", 87), ("Keerthana", "Physics", 93), ("Keerthana", "Chemistry", 91), ("Keerthana", "English", 74), 
  ("Harsha", "Maths", 56), ("Harsha", "Physics", 65), ("Harsha", "Chemistry", 71), ("Harsha", "English", 68), 
  ("Vidya", "Maths", 86), ("Vidya", "Physics", 62), ("Vidya", "Chemistry", 75), ("Vidya", "English", 83), 
  ("Komala", "Maths", 63), ("Komala", "Physics", 69), ("Komala", "Chemistry", 64), ("Komala", "English", 60)], 3)
 
# Sequence operation : Finding Maximum Marks from a single partition
# accumulator = 0, element = (subject, marks)

#####################################################
# Ex 1: Print Max Marks per Student                 #
# accumulator: 0, element: (subject, marks)         #
#####################################################

def seq_op(accumulator, element):
    if(accumulator > element[1]):
        return accumulator 
    else: 
        return element[1]
 
 
# Combiner Operation : Finding Maximum Marks out of Partition-Wise Accumulators
def comb_op(accumulator1, accumulator2):
    if(accumulator1 > accumulator2):
        return accumulator1 
    else:
        return accumulator2
 
# Zero Value: Zero value in our case will be 0 as we are finding Maximum Marks
zero_val = 0
aggr_rdd = student_rdd.map(lambda t: (t[0], (t[1], t[2])))

aggr_rdd2 = aggr_rdd.aggregateByKey(zero_val, seq_op, comb_op) 
 
# Check the Outout
for tpl in aggr_rdd2.collect():
    print(tpl)
 
#####################################################
# Ex 2: Print Subject name along with Max Marks     #
# accumulator: ('', 0), element: (subject, marks)   #
#####################################################
 
# Defining Seqencial Operation and Combiner Operations
def seq_op(accumulator, element):
    if(accumulator[1] > element[1]):
        return accumulator 
    else: 
        return element
 
 
# Combiner Operation : Finding Maximum Marks out Partition-Wise Accumulators
def comb_op(accumulator1, accumulator2):
    if(accumulator1[1] > accumulator2[1]):
        return accumulator1 
    else:
        return accumulator2
    
 
# Zero Value: Zero value in our case will be 0 as we are finding Maximum Marks
zero_val = ('', 0)
aggr_rdd = student_rdd.map(lambda t: (t[0], (t[1], t[2]))) 

aggr_rdd2 = aggr_rdd.aggregateByKey(zero_val, seq_op, comb_op)  

# Check the Outout
for tpl in aggr_rdd2.collect():
    print(tpl)
 
 
######################################################
# Ex 3: Print over all percentage of all students   #
# accumulator: ('', 0), element: (subject, marks)   #
######################################################
 
# Defining Seqencial Operation and Combiner Operations
def seq_op(accumulator, element):
    return (accumulator[0] + element[1], accumulator[1] + 1)
    
 
# Combiner Operation : Finding Maximum Marks out Partition-Wise Accumulators
def comb_op(accumulator1, accumulator2):
    return (accumulator1[0] + accumulator2[0], accumulator1[1] + accumulator2[1])
    
 
# Zero Value: Zero value in our case will be 0 as we are finding Maximum Marks
zero_val = (0, 0)
aggr_rdd = student_rdd.map(lambda t: (t[0], (t[1], t[2])))   \
                      .aggregateByKey(zero_val, seq_op, comb_op)  \
                      .map(lambda t: (t[0], t[1][0]/t[1][1]*1.0))
  
 
# Check the Outout
for tpl in aggr_rdd.collect():
    print(tpl)
    
sc.stop()