# -*- coding: utf-8 -*-
"""
Created on Thu Jan 30 10:54:20 2019

@author: Kanakaraju
"""
#===============================
# Range object
#===============================
r1 = range(5)
print ( r1, type(r1) )

r2 = range(30, 0, -5)
print ( r2, type(r2) )

for i in r2:
    print (i, end = " ")
    
# ==============================
# Tuples inside Lists
# ==============================

list1 = [(1,4,3,2), ('python', 'java', 'scala')]

print ( len(list1) )
print ( max(list1[0]) )
print ( len(list1[1]) )

print ( list1[1][1:] )

print ( list1[1][::-1][1][::-1] )  # guess whats the output?

print ( sorted(list1) )  # will it work?
print ( sorted(list1[0]) )
print ( sorted(list1[0], reverse=True) )
print ( sorted(list1[1]) )
print ( sorted(list1[1], reverse=True) )


tuple1 = list1[0] + list1[1]
print ( tuple1 )

del ( tuple1 )
#print ( tuple1 )     # can you print a deleted tuple?


# ==============================
# Tuples Vs Lists
# ==============================
list2 = [1, 3, 5, 'python', 'java']
print(list2)
list2[2] = 50
print(list2)

tuple2 = (2, 4, 6, 'python', 'machine learning')
print(tuple2)
tuple2[2] = 60  # won't work. Tuple does not support item assignment as they are immutable
print(tuple2)

str1 = "data science"
print( str1 )
print( str1[6] )
str1[6] = "C"  # won't work. Strings are immutable
print( str1[6] )


# ==============================
#  Lists inside Tuples
# ==============================
t1 = ([1,2,3,4], [3,4,5,6,7,8,9,10], [5,6,7,8])
print(t1)
print(len(t1))
print(t1[1][2])
print(t1[1][0:len(t1):2])
print(t1[1][::-2])

t1[0][1] = 20 # Since tuple contains lists, the overall tuple becomes mutable
print(t1)

# ==============================
#  Converting between Lists & Tuples
# ==============================
tuple1 = (1, 2, 3, 'a', 'b')
list1 = [10, 20, 30,'java', 'python']

print (type(tuple1))
print (type(list1))

list2 = list(tuple1)
print (type(list2))
print (list2)

tuple2 = tuple(list1)
print (type(tuple2))
print (tuple2)

# updating elements of a tuple 
#tuple1[1] = 20  # Not allowed
tmp_list = list(tuple1)
tmp_list[1] = 20
tuple1 = tuple(tmp_list)
print (tuple1)

# ==============================
#  Membership Checking
# ==============================
tuple1 = (1, 2, 3, 'a', 'b')
list1 = [10, 20, 30,'java', 'python']

print (1 in tuple1)
print ('java' in list1)
print ('php' in list1)
print ('e' in 'test')

#===============================
# Tuples and Lists in Dictionary
#===============================
dict1 = { 1: (1,2,3), 10: (10,20,30) }
print ( dict1 )

# NOTE: Dictinary indexes start with 1 (not 0)
print(dict1[0])  # Error: 0 index not allowed
print(dict1[1])
print(dict1[2])


print ( dict1[1][1] )
print ( dict1[2][1] )
print ( sum(dict1[2]), max(dict1[2]), min(dict1[2]) )


# dictionary of lists - values are accesses by key-value, not key-index
dict2 = { 1: [1,2,3], 10: [10,20,30] }
print ( dict2 )
print ( type(dict2) )
print ( type(dict2[1]) )

print(dict2[1])
print(dict2[10])
print(dict2[10][0])








