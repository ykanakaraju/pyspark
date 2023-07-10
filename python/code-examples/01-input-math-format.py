# -*- coding: utf-8 -*-

#Reading data from the standard-input/keyboard
name = input("What's your name? ")
print("Hello " + name + "!")

age = input("Your age? ")
print( name + " is " + str(age) + " years old")

cities = input("Largest cities in India: ")
print(cities, type(cities))

#cities = eval(input("Largest cities in India: "))
#print(cities, type(cities))

population = input("Population of India? ")
print(population, type(population))

population = int(input("Population of India? "))
print(population, type(population))

#accept multiple input values
num_1, num_2 = input("Enter two numbers: ").split()
num_1 = int(num_1)
num_2 = int(num_2)

sum_1 = num_1 + num_2
diff = num_1 - num_2
prod = num_1 * num_2
quotient = num_1 / num_2
remainder = num_1 % num_2

print("{} + {} = {}".format(num_1, num_2, sum_1))
print("{} - {} = {}".format(num_1, num_2, diff))
print("{} * {} = {}".format(num_1, num_2, prod))
print("{} / {} = {}".format(num_1, num_2, quotient))
print("{} % {} = {}".format(num_1, num_2, remainder))


#math functions

import math

print("ceil(3.2) = ", math.ceil(3.2))
print("floor(3.2) = ", math.floor(3.2))
print("fabs(-3.2) = ", math.fabs(-3.2))
print("pow(3, 2) = ", math.pow(3, 2))    # 3 ** 2
print("sqrt(36) = ", math.sqrt(36))    # 3 ** 2
print("fmod(3, 2) = ", math.fmod(3, 2))  # 3 % 2
print("factorial(5) = ", math.factorial(5))
print("trunc(3.2) = ", math.trunc(3.2)) 

print("log(1000, 10) = ", math.log(1000, 10)) 
print("log10(1000) = ", math.log10(1000)) 
print("radians(180) = ", math.radians(180))
print("degrees(3.141592653589793) = ", math.degrees(3.141592653589793))
print("sin(0) = ", math.sin(0))
print("math.e = ", math.e) 
print("math.pi = ", math.pi) 





