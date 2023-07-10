# -*- coding: utf-8 -*-
"""
Created on Sat Feb 23 16:39:15 2019

@author: Kanakaraju
"""

######################################################
'''
# Script 1: json parsing
'''
import json

# some JSON:
x =  '{ "name":"John", "age":30, "city":"New York"}'

# parse x:
y = json.loads(x)

# the result is a Python dictionary:
print(type(y))
print("age:", y["age"])


######################################################
'''
# Script 2: creating json usign python
'''

json_str = {
  "name": "John",  "age": 30,  "married": True,  "divorced": False,
  "children": ("Ann","Billy"),  "pets": None,
  "cars": [
    {"model": "Ford Edge", "mpg": 24.1},
    {"model": "BMW 230", "mpg": 27.5}    
  ]
},{
  "name": "Adam",  "age": 32,  "married": True,  "divorced": False,
  "children": ("Peter","Shay"),  "pets": None,
  "cars": [
    {"model": "Toyota Camri", "mpg": 32.5},
    {"model": "Honda Civic", "mpg": 34.1}
  ]
}

# convert into JSON:
js = json.dumps(json_str, indent=2, sort_keys=True)

# the result is a JSON string:
print(js)