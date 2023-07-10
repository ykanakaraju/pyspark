# -*- coding: utf-8 -*-
"""
Created on Fri Feb  1 08:55:50 2019

@author: Kanakaraju
"""

# Example 1: Factorial

def factorial(num):
    if num <= 1:
        return 1
    else:
        result = num * factorial(num -1)
        return result
        
print(factorial(6))


