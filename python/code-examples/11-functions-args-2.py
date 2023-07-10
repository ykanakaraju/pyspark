# -*- coding: utf-8 -*-
"""
Created on Fri Feb  1 08:55:50 2019

@author: Kanakaraju
"""

print (range(3,7))

#if you have the arguments of the range function above as a list
#then you can unpack the list and pass the contents of the list
#as arguments to range using * notation (i.e as arbitrary args list)

lst = [3, 7]
print ( range (*lst) )
print (*lst)

# you can use ** notation to unpack a dictionary into keyword argumnets

def myAddress(location, city='Hyderabad', state='Telangana'):
    print("Location: ", location)
    print("City: ", city)
    print("State: ", state)
 
addr = {"location": "Sundar Akash Apartments", "city": "Pune", "state": "Maharashtra"}
myAddress(**addr) 


