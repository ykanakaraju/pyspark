# -*- coding: utf-8 -*-

d1 = {}
d2 = dict()
d3 = {1: 'one', '2':'two', 3.5:'three', 4:4}

print( d3 )
print( d3[1])

print( d3.keys(), type(d3.keys()) )
print( d3.values(), type(d3.values()) )
print( d3.items(), type(d3.items()) )


#check if a key exists in a dict using "in"
print("Is there a key 3.5? ", 3.5 in d3)

d3['five'] = 5  # add/update an item
d3[1] = 1
print (d3)


for k,v in d3.items() :
    print (k, v)
    
d3.pop('five')    
del d3[3.5]

print(d3)

d3.get('5', None)
d3.get('4', None)
d3.get('5', 'key 5 not found')

sorted(d3)  # not supported on dicts having keys with mixed types

d3.setdefault(1, 5)   # if key does not exists, it will be created with default value of 5
d3.setdefault(17, 5) 
d3.setdefault(15, 'fifteen')
print(d3)

del d3[1]
del d3[17]
d3['1'] = 10
d3


'''
Sorting the dictionary
Sorting only works with same key types
If you attempt to sort by values, values should be of same type too.
'''
d4 = { 3:'three', 4:'four', 1:'one', 2:'two', 5:'five'}

print( max(d4), min(d4) )
print( d4[max(d4)], d4[min(d4)] )

lkeys = sorted(d4)
print( type(lkeys), lkeys )

lvalues = sorted(d4.values())
print( type(lvalues), lvalues )

litems = sorted(d4.items()) 
print( type(litems), litems )

#sorting the dicionary based on values
#key: specifies the sort key, here sorting by second key i.e value
ldictvalues = sorted(d4.items(), key=(lambda pair: pair[1])) 
print( type(ldictvalues), ldictvalues )

# get a dictionary sorted by values (instead of a list)
dictvals = dict(sorted(d4.items(), key=(lambda pair: pair[1])))  # get a dictionary.
print( type(dictvals), dictvals )

d5 = d4.copy()
print( d4, d5 )
print( id(d4), id(d5) )

'''
dictionary comprehension
Similar to list comprehensions, but allow you to easily construct dictionaries
dictionary comprehension uses { .. } where as list comprehension uses [..]
'''
nums = [0, 1, 2, 3, 4, 5, 6, 7, 8]

# dictionary comprehension uses { .. } where as list comprehension uses [..]
even_num_to_square = {x: x ** 2 for x in nums if x % 2 == 0}
print(even_num_to_square) 


d = {0:1, 1:2, 2:3, 10:4, 11:5, 12:6, 100:7, 101:8, 102:9, 200:10, 201:11, 202:12}
keys = (0, 1, 2, 100, 101, 102)
dict1 = {k: d[k] for k in keys}
print(dict1)

#zip method
list_1 = ['a', 'b', 'c', 'd']
list_2 = [1, 2, 3]
dict_1 = {}

for (key, value) in zip(list_1, list_2):
    dict_1[key] = value 
  
print("Output Dictionary using for loop:", dict_1) 