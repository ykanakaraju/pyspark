# -*- coding: utf-8 -*-
name = "john"   # A string
age = 25        # An integer assignment
height = 5.8    # A floating point

#assigning values to multiple variables
name, age, height = "raju", 26, 5.9
a = b = c = 100

#Using print
print(a)

print(name, age, height)
print(name, age, height, sep="||")
print(name, age, height, end="")  #changing default end char (which is '\n')
print("There is no line break")
print("I am %s, %d years old and %4.2f tall" % (name, age, height))

print(f"I am {name}, {age} years old and {height} tall")

#Using escape sequences
my_name = "\"My name is John\""
print(my_name)

############################
#   Print formatting
############################
print("Name: {}, Age: {}, Height: {}".format(name, age, height) )

# show integer with min 3 digits. append leading zeros if required
i1 = 5
print("{:03d}".format(i1))  

# show float with 2 decimal places
f1 = 5.6 
print("{:.2f}".format(f1))  


