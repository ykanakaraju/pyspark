# -*- coding: utf-8 -*-
l1 = [98,44,67,98,56,34,67,89,12,55,66,77,55,66,77]
l2 = []
for mark in l1:
    if mark not in l2:
        l2.append(mark)
print(l2)
print(len(l2), len(l1))

# The simplest way to do the above is to convert the list to a set
s1 = set(l1)
print(s1)

s2 = {98,99,55,78,67,56,45,44}

# ============ Set Operations ============
print(s1)
print(s2)
#intersection
s3 = s1 & s2   # creates a new set
s3a = s2.intersection(s1)
print(s3, id(s3))
print(s3a, id(s3a)) 

#union
s4 = s1 | s2
s4a = s1.union(s2)
print(s4)

#difference
s5 = s1 - s2  
s5a = s1.difference(s2)
print(s5)
#print(s5a) #this is same as above

# issubset & issuperset
print( s1.issubset(s2), s5.issubset(s1), s1.issuperset(s5) )

# symmetric_difference
s6 = s1.symmetric_difference(s2)
s6a = (s1 - s2).union(s2 - s1)   #meaning of symmetric_difference
s6b = s1.union(s2) - s1.intersection(s2)
print(s6)
print(s6a)
print(s6b)

 # removes an element orbitrarily
s1.pop() 

'''==========================================
          Complex Sets (Sets of sets)
==========================================='''

maths = s1
english = s2

cd = {1:maths, 2:english, 3:s2}
print(cd)

''' 
you can not fetch set values by indexing
convert them into list/dictionary etc to do that
'''
cdl = list(cd.items())
print(type(cdl))

print( cdl[0], type(cdl[0]) )
print( cdl[0][1], type(cdl[0][1]) )

print( list(cdl[0][1])[2] )




#