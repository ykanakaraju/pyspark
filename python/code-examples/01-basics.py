# -*- coding: utf-8 -*-
    
"""
multi line 
comments
"""

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

#Using escape sequences
my_name = "\"My name is John\""
print(my_name)

#Print formatting
i1 = 5
# show integer with min 3 digits. append leading zeros if required
print("{:03d}".format(i1))  


multi_line = """ This is
a muliti line 
string"""

print(multi_line)


# Basic numerical ops
2 + 3
5 / 2
5 // 2  # integer division
5 ** 3  # power
5 % 3   # modulus
 


