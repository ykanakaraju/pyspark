# -*- coding: utf-8 -*-
'''=====================================
==========  Tuples =====================
======================================'''

t1 = (1, 2.5, 'python', 2.0, 1.23, 'hello', 4, 6, 7);  
print(t1)

t2 = (11, 12.5, 'machine learning', t1);  
print(t2)

print(t1[1], t1[-1], t2[-2], t2[3][0], t2[-1][-2])

print(t1[1:4])
print(t1[1:-1])
print(t2[-1][-2:])
print(t2[3][2:])
print(t1[0:len(t1)])
print(t1[0:len(t1):2])
print(t1[::-1])
print(t1[6:1:-2])


# Convert a List into a Tuple
a_list = [55, 89, 144]
a_tuple = tuple(a_list)

# Convert a Tuple into a List
a_list = list(a_tuple)
print("Min :", min(a_list))
print("Max :", max(a_list))

# Sorting a tuple
t3 = (1,5,3,9,6,4,7)

print( sorted(t3) )
print( sorted(t3, reverse=True) )

# sorted method returns a list object
l3 = sorted(t3, reverse=True)
print ( type(l3) )

#converting a list to a tuple
t31 = tuple(l3)
type(t31)