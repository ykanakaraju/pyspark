# -*- coding: utf-8 -*-

'''=====================================
==========  Strings =====================
======================================'''

str1 = 'welcome to Python.'
str2 = "Machine Learning is cool."

print(str1, str2)
print(str1 + str2)
print(str1 * 3)
print(f"Length of '{str1}', is {len(str1)}" )

# string slicing
print(str1[0], str1[1], str1[5])
print(str1[1:5])
print(str1[:15])
print(str1[3:])

# unicode string
unicodeStr = u"This is unicode string. తెలుగు హిందీ"
print(unicodeStr)

# string replacement
str3 = str1.replace(str1, str1.upper())
str4 = str1.replace("Python", "Data Science")
print(str3, str4)

#instance: Check for instance comparision
isinstance(str3, str) 
isinstance(str3, int) 

print ( max(str1) )
print ( min(str1) )
print ( str1.replace(".", "!!") )
print ( str1.upper() )
print ( str1.capitalize() )

print ( str1.find('e') )
print ( str1.find('E') )
print ( str1.index('E') )  #observe the difference between find and index

print ( str1.index('e') )
print ( str1.index('e', 4) )
print ( str1.index('E') )   # throws an error if not found. use 'find' instead.

print ( str2.count('i', 0, len(str2)) )
print ( str2.count('i') )
print ( str2.count('i', 10) )