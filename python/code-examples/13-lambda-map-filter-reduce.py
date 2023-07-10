# -*- coding: utf-8 -*-
"""
Created on Thu Jan 31 18:32:25 2019

@author: Kanakaraju
"""

'''
# snippet 1
# map : map takes two argumnets
# agr 1 : a function definition
# arg 2 : an iterable collection
# returns : an iterable collection
'''
marks = [34, 78, 56, 90, 55, 80]
iter1 = map(lambda x: 'A' if x >= 70 else 'B', marks)
print ("TYPE:", type(iter1))
list1 = list(iter1)
print(str(list1))

'''
# snippet 2
# filter
'''
marks = [34, 78, 56, 90, 55, 80]

marks2 = filter(lambda x: x >= 60, marks)

#print(list(marks2))

# * unpacks a list and gives the elements as arguments to the function.
# refer to ex 11.function-args-2.py
print(*list(marks2))

for i in filter(lambda x: x >= 60, marks):
    print(i)



'''
# snippet 3
# reduce
'''
from functools import reduce

marks = [34, 78, 56, 90, 55, 80]
reduceMarks = reduce(lambda x, y: x + y, marks)
reduceFactorial = reduce(lambda x, y: x*y, range(1,6))     #range(1,6) => 1,2,3,4,5
print(reduceMarks, reduceFactorial)

'''
# snippet 4
# Passing functions to map and filter
'''
def isGt60(mark):
    return mark >= 50

marks3 = filter(isGt60, marks)  
print(*list(marks3))

marksMap4 = map(isGt60, marks)
print(*list(marksMap4))