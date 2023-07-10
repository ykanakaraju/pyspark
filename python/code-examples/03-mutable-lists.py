# -*- coding: utf-8 -*-
"""
Created on Mon Jan 28 12:42:04 2019

@author: Kanakaraju
"""

l1 = []
l2 = list()

type(l2)

l2 = [12, "python", 20.5, 1, 10.3]
print(l2, l2[1], id(l2))

#getting index of a list element
print(l2.index(20.5))

#looping a  list along with index
for i in l2:
    print("{}: {}".format(l2.index(i), i))

l2[0] = 22
print(l2, id(l2))

tt1 = tuple(l2)

t1 = l2[0], l2[1], l2[2]    # list elements to tuple
type(t1)

print(t1)

print( list(t1) )    # tuple to a list

l2.extend([10.23, 'Bill'])

print(l2[0], l2[-1], l2[0:len(l2):2], l2[::-1])

# print (sorted(l2))
#### sorted function does not work with mixed (data type) lists

l3 = [5.3, 7, 1.2, 10.3, 4, 3.2, 12.1, 4]
print(l3, sorted(l3), id(l3), id(sorted(l3)))

l3.append(40)    # appends to the list 
print( l3, id(l3) ) #observe that the id did not change as lists are mutable
#l3.append([1,2])

l3.extend([10, 20])
print( l3, id(l3) )

l3.insert(2, -10.4)     # insert an element at an arbitrary location
print( l3, id(l3) )

l3.pop()                # removes the last element
print( l3 )

l3.pop(4)               # removes the element at index: 4
print( l3 )

l3.remove(7)            # removes the first occurance of an element based on value
print( l3 )

del(l3[0])              # deletes an element by index
print( l3 )

l5 = [10,20,30,10,20,30,10,20,30]
l5.remove(10)
del(l5[2])
l5

# nested lists
nestedList = []
nestedList.append(['Guido', 'Rossum', 40])
nestedList.append(['Donald', 'Trump', 70])
nestedList.append(['Barack', 'Obama', 70])

nestedList

nestedList[0]

nestedList[0][1]

nestedList[-1][1]

nestedList[::-1]

nestedList[0:len(nestedList)]

nestedList[1:len(nestedList)][::-1]

nestedList[::][1][0]

nestedList[::][1][0][0]   #you can iterate over the list

'Donald' in nestedList

'Donald' in nestedList[::][1]


# list comparision
l3.append(-43)
l3

#remove all negative vals from l3
l4 = [val for val in l3 if val > 0]
l5 = [val+10 for val in l3 if val > 0]
l4
l5

l6 = (val+1 for val in l3 if val > 0)
l6

for x in l6:
    print(x)
    
l7 = tuple([val+1 for val in l3 if val > 0])
l7


'''
The range() function returns a sequence of numbers, 
starting from 0 by default, and increments by 1 (by default), 
and ends at a specified number.
'''
print( list(range(1,20,2)) )

'''
Observer here: range1 is a generator that emits values as it is iterated over
though rang1 has potentially 10 trillion values, that many values 
are not allocated at assignment that could cause resource exhaustion.
'''

#range1 = range(1,10000000000000)
range1 = range(1,1000)
for x in range1:
    if x < 10:
        print(x)

'''
List Comprehension
'''
#separate each letter in a string and put all letters in a list object

#lll = list('Python is Cool')
#normal way without using list comprehension
chars=[]
for ch in 'Python is Cool':
    chars.append(ch)
print(chars)


#+++++++++++++++++++++++++++++++++++++++++++++
#  using list comprehension...
#+++++++++++++++++++++++++++++++++++++++++++++

chars = [ch for ch in 'Python is Cool']
print(chars) 

#range(11) => 0 to 10
squares = [x*x for x in range(11)] 
print(squares)    

list1=[1,2,3]
list2=[4,5,6]
combine_lists =[(x,y) for x in list1 for y in list2]
print(combine_lists) 

list3 = [x for x in range(21) if x%2==0]
print(list3)

list4 = [x for x in range(21) if x%2==0 if x%5==0]
print(list4)

list5 = ["Even" if i%2==0 else "Odd" for i in range(10)]
print(list5)

matrix=[[1, 2, 3], [4, 5, 6], [7, 8, 9]]
flat_list=[num for row in matrix for num in row]
print(flat_list)

import math
num_list = [1,2,3,4,5]
list_of_values = [[math.pow(m, 2), math.pow(m, 3), math.pow(m, 4)] for m in num_list]



#+++++++++++++++++++++++++++++++++++++++++++++
# multi-dimensinal lists
#+++++++++++++++++++++++++++++++++++++++++++++

import math

# list multiplication (can be used to initialize multi-dimensional lists)
list_stars = [["*"] * 3 for i in range(4)]
print(list_stars)


# List of lists containing values to the power of 2, 3, 4
num_list = [1,2,3,4,5]
list_of_values = [[math.pow(m, 2), math.pow(m, 3), math.pow(m, 4)] for m in num_list]

for k in list_of_values:
    print(k)
print()

# Create a 10 x 10 list
multi_d_list = [[0] * 10 for i in range(10)]

# Change a value in the multidimensional list
multi_d_list[0][1] = 10

# Get the 2nd item in the 1st list
# It may help to think of it as the 2nd item in the 1st row
print(multi_d_list[0][1])

# Get the 2nd item in the 2nd list
print(multi_d_list[1][1])

# Multidimensional Lists
#
# Multidimensional list are tables of data that spans across rows and columns
# Here I’ll show how indexes work with a multidimensional list

# Create the multidimensional list 10 x 10
list_table = [[0] * 10 for i in range(10)]

# Fill the list with values
for i in range(10):

    for j in range(10):
        list_table[i][j] = "{} : {}".format(i, j)

for i in range(10):
    for j in range(10):
        print(list_table[i][j], end=" || ")
    print()
