# -*- coding: utf-8 -*-
"""
Created on Sat Feb 23 16:39:15 2019

@author: Kanakaraju
"""

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
      print("File Handler f does not exists")
