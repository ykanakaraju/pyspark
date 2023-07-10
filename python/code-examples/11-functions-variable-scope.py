# -*- coding: utf-8 -*-
 
#---------------------------------------------------
#  Example 1
#---------------------------------------------------
# Here x is a global variable
x = 100

def my_function():
  print(x)

my_function()

print(x)

#---------------------------------------------------
#  Example 2
#---------------------------------------------------
# Here x and y are global variable
x = 100
y = 200
def my_function_2():
    
  x = 10  # x is local variable  
  print(x)
  
  print(y)

my_function_2()

#---------------------------------------------------
#  Example 3
#---------------------------------------------------
i = 100
def my_function_3():
  j = 1
  def my_inner_function_3():
    print(i, j)
    
  my_inner_function_3()


my_function_3()

#---------------------------------------------------
#  Example 4
#---------------------------------------------------
def change(name):
    name = "Kanak"
    print("inside function: " + name)
    
name = "Raju"
change(name)
print("outside function: " + name)

#---------------------------------------------------
#  Example 5
#  If you use the global keyword, the variable 
#  belongs to the global scope
#---------------------------------------------------
def change_global():
    global name 
    name = "Kanak"
    print("inside function: " + name)
    
name = "Raju"
change_global()
print("outside function: " + name)

#---------------------------------------------------
#  Example 6
#  Function can return multiple values
#---------------------------------------------------
def quotient_reminder(a, b):
    return a // b, a % b

q, r = quotient_reminder(10, 3)
print(q, r)      






