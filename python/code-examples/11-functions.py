# -*- coding: utf-8 -*-
#================================== 
# Example 1
#================================== 
def fun(a, b, c):
    print (a, b, c)
    return()
    
fun(10, 20, 30)          # positional args
fun(a = 10, c=30, b=20)  # named args

#================================== 
# Example 2 - Default Args   
#================================== 
def funDefault(a, b, c=100):
    print (a, b, c)
    
funDefault(10, 20, 30)
funDefault(10, 20)       # function used default value, when the arg is not supplied
funDefault(a=10, c=20, b=30)
funDefault(a=10, b=30)

#=========================================================
# Example 3: *args accepts arbitrary number of arguments
#=========================================================
def sum_all(*args):
    sum_1 = 0
    for i in args:
        sum_1 += i        
    return sum_1
    
print("sum: " , sum_all(1,2,3,4,5))   

#==================================================    
# Example 4: function with arbitrary arguments
# *args is treated as a tuple
#==================================================
def funArbitrary(a, b, c=20, *args):
    print(a, b, c)
    print(args)
    print(a + b + c + sum(args))
    
funArbitrary(32, 44, 120, 23, 34, 45)
# funArbitrary(a=32, b=44, c=120, 23, 34, 45)   # This one fails. We can't use named args here

 
#==================================================
# function with arbitrary arguments and keyword arguments 
# *kwargs is treated as a dictionary i.e k:v pairs  
#==================================================
def funKeywordArgs(a, b, c=20, *args, **kwargs):
    print(a, b, c)
    print(args)
    print(kwargs)
    #print(a + b + c + sum(args))

funKeywordArgs(10, 15, 20, 25, 30, 35, x=20, y=30)





