# -*- coding: utf-8 -*-
# Given two lists containing the names of states and their 
# corresponding capitals, construct a dictionary which maps 
# the states with their respective capitals.

state = ['Tamilnadu', 'Maharashtra', 'Telangana'] 
capital = ['Chennai', 'Mumbai', 'Hyderabad'] 
  
output_dict = {} 
  
# zip command
# If you use zip() with n arguments, then the function will return 
# an iterator that generates tuples of length n
zipped = zip(state, capital)
type(zipped)
print(list(zipped))

# Using loop for constructing output dictionary 
for (key, value) in zip(state, capital): 
    output_dict[key] = value 
  
print("Output Dictionary using for loop:", output_dict) 