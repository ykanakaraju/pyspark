# -*- coding: utf-8 -*-
"""
Created on Thu Jan 31 18:32:25 2019

@author: Kanakaraju
"""

# Snippet 1
sq = lambda x :  x**2
print (sq(4))

# Snippet 2
sq2 = lambda x : x**2 if x > 10 else x**3
print(sq2(5))
print(sq2(15))

#snippet3
# lambda with multiple args
add = lambda x,y : x + y
print (add(10, 20))


#snippet 4
# function returning a lambda
def multiplier(n):
  return lambda a : a * n

mydoubler = multiplier(2)
mytripler = multiplier(3)

print(mydoubler(11))
print(mytripler(11))
