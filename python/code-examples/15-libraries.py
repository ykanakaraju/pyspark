# -*- coding: utf-8 -*-
"""
Created on Sat Feb 23 16:39:15 2019

@author: Kanakaraju
"""

import platform
import os

'''
# Script 1: print all platform variable list
'''
x = dir(platform)
print(x)

######################################################
'''
# Script 2: import local python files
'''
# get the directory of the current python file
os.path.dirname(os.path.realpath('__file__'))

# switch to appropriate working directory before importing local files
os.chdir(os.path.dirname(os.path.realpath('__file__')))
os.getcwd()

from module1 import fun, person1

fun()
print(person1["name"])

######################################################
'''
# Script 3: sys module
'''
import sys

dir(sys)
print( sys.version )
print( sys.version_info )

# version number used to form registry keys 
print( sys.winver )

for i in (sys.stdin, sys.stdout, sys.stderr):
    print(i)

######################################################
'''
# Script 3: os module
'''
import os

os.getlogin()

os.environ

os.getenv('PROCESSOR_IDENTIFIER')

os.getcwd()    # get current working directory
os.getcwdb()   # gives you bytes object

os.chdir('E:\\Data Science')  # change directory

os.listdir() # list directories and files

os.mkdir('dir1')  # create a directory

os.rename('dir1','dir2')  # rename a dir or a file

os.remove('dir2')  # requires permissions

######################################################
'''
# Script 4: os.path module
'''

import os.path

print( os.path.abspath(".") )

print( os.path.basename("E:/Data Science") )

print( os.path.normpath("E:/Data Science") )

print( os.path.commonprefix(["E:/datafiles", "E:/datasets"]) )

print( os.path.realpath('__file__') )

print( os.path.dirname(os.path.realpath('__file__')) )

print( os.path.exists("C/Test") )

######################################################
'''
# Script 5: math module
'''

import math

print (math.factorial(5))
print (math.ceil(5.2), math.floor(5.2), sep=",")
print (math.fabs(-10.5))
print (math.pow(10, 3)) 
print (math.exp(3)) 
print (math.pi) 
print (math.sin(math.radians(90)))
print (math.cos(math.pi))
print (math.tan(math.radians(45)))


######################################################
'''
# Script 6: datetime module
'''

import datetime
from dateutil import parser, _version

print(datetime.datetime.today())   # today
print(datetime.datetime.today() - datetime.timedelta(1)) # subtract 1 day
print(datetime.date.today() - datetime.timedelta(1))     # show only the date part

print(datetime.datetime.timestamp())

print(_version.VERSION)

print(parser.parse('10-10-2018'))  # converts any valid date string into a datetime object
print(type(parser.parse('10-10-2018')))

# time difference
now = datetime.datetime.today()
othertime = datetime.datetime(1975, 8, 31)
print(othertime)

print(now - othertime) 


######################################################
'''
# Script 6: time module
'''
import time

ticks = time.time()
print ("Number of ticks since 12:00am, January 1, 1970:", ticks)

#asctime takes timetuple as argument
localtime = time.asctime( time.localtime(time.time()) )
print ("Local current time :", localtime)

print ("Sleeping for 5 seconds...")
time.sleep(5)

localtime2 = time.ctime()  #simpler version of asctime
print ("Local current time :", localtime2)

localtimetuple = time.localtime(time.time())
print (type(localtimetuple))
print (localtimetuple[0:len(localtimetuple)])

######################################################
'''
#script 7: calendar module
'''
import calendar

cal = calendar.month(2009, 1)
print ("Here is the calendar:")
print(cal)

######################################################
'''
#script 8: random module
'''
import random

# gets random number from a range
print ( random.randrange(0, 100, 10) )

# gets random number between two numbers
print ( random.randint(1, 100) )

#sampling without replacement
print ( random.sample([1,2,3,4,5,6,7,8,9], 3) )
print ( random.sample(range(10, 20), 5) )

#sampling with replacement
print ( random.choices(["R", "G", "B", "Y"], k=3) )

# returns a random float between two numbers
print ( random.uniform(2, 6) )

