# -*- coding: utf-8 -*-

import sys

# Immutable data types - integer value is immutable

'''=====================================
== Integers, Float & Complex ===========
======================================'''

#immutability of variables
a = 10
print ( id(a),  type(a))

b = 10
print ( id(b),  type(b), (a is b) )

a = a + 1
print ( id(a), id(b), a, b, (a is b) )

a = a - 1
print ( id(a), id(b), a, b, (a is b) )

c = a; 
print ( id(a), id(c), a, c, (a is c) )

c = c + 1
print ( id(a), id(c), a, c, (a is c) )

# numeric types
x = 5; print (x, type(x))          # integer

y = 6.25; print (y, type(y))       # decimal

#format float type
print("{} formatted to two decimal places is {:.2f}".format(1.23456, 1.23456))

z = 2 + 3j; print (z, type(z))     # complex

print(y * z)

#checking the max values of ints & floats
print(sys.maxsize)
print(sys.float_info.max)

#Floats are guaranteed to accuratly store only upto 15 decimal places
f1 = 1.1111111111111111
f2 = 1.1111111111111111
f3 = f1 + f2
print(f3)   # the value is gonna be wrong


# BInary, hexadecimal and octal formats
octal_val = 0o12
hexa_val = 0x3e
binary_val = 0b11100

#prints the above values in decimal format
print(octal_val, hexa_val, binary_val)

# converting a decimal value to bin, oct & hex
dec_val = 344
print("The decimal value of", dec_val, "is: ")
print(bin(dec_val),"in binary.")
print(oct(dec_val),"in octal.")
print(hex(dec_val),"in hexadecimal.")

print("\n")

print("The decimal value of '0x3c' is", int("0x3c", 16))
print("The decimal value of '0b11100' is", int("0b11100", 2))
print("The decimal value of '0o72' is", int("0o72", 8))




'''=====================================
========== Accurate Decimals ===========
========== decimals module =============
======================================'''

# Decimals can go upto 28 decimal places instaed of 15.

from decimal import Decimal as D

sum = D(0)
sum += D("0.0111111111111111111")    
sum += D("0.0111111111111111111")
sum += D("0.0111111111111111111")
print("Sum = ", sum)
sum -= D("0.0333333333333333333")
print("Sum = ", sum)

