# -*- coding: utf-8 -*-
"""
Created on Tue Jan 29 10:41:34 2019

@author: Kanakaraju
"""
#============================================= 
# for comprehension
#============================================= 

import math

#=============================================
# list comprehension
# list of even numbers
#=============================================
input_list = [1, 2, 3, 4, 4, 5, 6, 7, 7] 

# Using loop for constructing output list 
output_list = [] 
for var in input_list: 
    if var % 2 == 0: 
        output_list.append(var)   
print("Output List using for loop:", output_list)

# using list comprehension  
list_using_comp = [var for var in input_list if var % 2 == 0]   
print("Output List using list comprehensions:", list_using_comp) 


#=============================================
# dictionary comprehension
#
# create an output dictionary which contains only 
# the odd numbers that are present in the input 
# list as keys and their cubes as values.
#=============================================
input_list = [1, 2, 3, 4, 5, 6, 7] 
  
output_dict = {} 
  
# Using loop for constructing output dictionary 
for var in input_list: 
    if var % 2 != 0: 
        output_dict[var] = var**3
  
print("Output Dictionary using for loop:", output_dict ) 

# using dictionary comprehension
dict_using_comp = {var:var ** 3 for var in input_list if var % 2 != 0} 
  
print("Output Dictionary using dictionary comprehensions:", dict_using_comp)


#=============================================
# set comprehension
#=============================================
input_list = [1, 2, 3, 4, 4, 5, 6, 6, 6, 7, 7] 
  
set_using_comp = {var for var in input_list if var % 2 == 0} 
  
print("Output Set using set comprehensions:", set_using_comp) 




    