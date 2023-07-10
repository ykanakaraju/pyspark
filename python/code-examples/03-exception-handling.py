# -*- coding: utf-8 -*-
"""
Created on Sat Feb 23 16:39:15 2019

@author: Kanakaraju
"""

#Example 1
while True:
    try:
        num = int(input("Enter an integer: "))
        break
    except ValueError:
        print("That is not a number. Try again")
    except:
        print("Unknown Error. Try again")
print("Thank you for enterig a number.")

#example 2
try:
    f = open("demofile.txt")
    f.write("Python is a good choice for data science")  
    
except FileNotFoundError as fne:
    print("demofile.txt does not exist", ". Error:", fne.strerror)   
    
except (NameError, KeyError) as e:  # exception group
    pass

except Exception as e:
    print(e.with_traceback)
    
finally:
  try:
      f.close()
  except NameError:
      print("File handler does not exists")
      
      
#example 3 - raising custom exceptions
class B(Exception):
    pass

class C(B):
    pass

class D(C):
    pass

for cls in [B, C, D]:
    try:
        raise cls()
    except D:
        print("D")
    except C:
        print("C")
    except B:
        print("B")
