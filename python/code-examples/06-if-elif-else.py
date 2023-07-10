# -*- coding: utf-8 -*-
"""
Created on Tue Jan 29 10:37:01 2019

@author: Kanakaraju
"""

age = int(input("Enter age:"))

if (not age > 0): 
    print(age)
    print('Invalid age')
elif(age > 0 and age <= 12): 
    print(age)
    print('Child')
elif(age >= 13 and age <= 19): 
    print(age)
    print('Teenager')
elif(age <= 35): 
    print(age)
    print('Youth')
elif(age <= 50): 
    print(age)
    print('Middle aged')
else: 
    print(age)
    print('Senior')
    
# Ternary Operator
print("====  Is Child? ==")   
str = 'Child' if(age >= 0 and age <= 12) else 'Not a child'   
print(str)
    