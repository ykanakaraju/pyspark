# -*- coding: utf-8 -*-
"""
Created on Wed Jan 30 12:15:36 2019

@author: Kanakaraju
"""
import sys
import os

# which version of python am I running?
print("Python " + sys.version)

# get current working directory
os.getcwd()

# change to the current script directory
os.chdir("C:\\Data Science\\01_Training_Material\\Code\\00-Python")

#======================================
# script 1; print fie contents
#======================================
f1 = open("anthem.txt")  # same as open("anthem1.txt", "r")
for line in f1:
    print(line.rstrip())
f1.close()

# The same can be written as follows:
with open("anthem.txt") as f1:
    for line in f1:
        print(line.rstrip())
        
#using readline to read each line at a time
with open("anthem.txt", encoding="utf-8") as my_file:
    lineNum = 1

    while True:
        line = my_file.readline()
        
        if not line:  # line is empty so exit
            break

        print(lineNum, " :", line, end="")

        lineNum += 1
#======================================
#script 2; Reading Unicode files
#======================================
f2 = open("anthem_hindi.txt", mode = "r", encoding = "utf-8")  
for line in f2:
    print(line.rstrip())
f2.close()

#======================================
#script 3 : writing to a file
#======================================

f_in = open("anthem1.txt", "r")
f_out = open("anthem1_out.txt", "w")  

i = 1
for line in f_in:
    f_out.write(str(i) + ": " + line)
    i = i + 1
    
f_in.close()
f_out.close()

# another way
with open("test.txt", 'w', encoding = 'utf-8') as f:
   f.write("my first file\n")
   f.write("This file\n\n")
   f.write("contains three lines\n")

#======================================
#script 3 : rename a file
#======================================
fh = os.rename("anthem1_out.txt", "anthem1_l.txt")


#======================================
#script 5 : deleting a file
#======================================
os.remove("test.txt")


#======================================
#script 6 : file attributes
#======================================
f = open("anthem1.txt", mode = "w+", encoding = "utf-8")
print(f.name)
print(f.closed) 
print(f.encoding) 
print(f.mode) 
print(f.newlines) 

#======================================
#script 6 : Working with Directories
#======================================
os.getcwd()    # get current working directory
os.getcwdb()   # gives you bytes object

os.chdir('E:\\Data Science')  # change directory
print( os.getcwd() )

os.listdir() # list directories and files

os.mkdir('Z0')  # create a directory
os.listdir()

os.rename('Z0','Z1')  # rename a dir or a file

os.remove('Z1')  # requires permissions
os.listdir()
#======================================
#script 6 : miscellanious
#======================================
fh = open("anthem1.txt")

fh.tell()  # tel me where the file pointer is
fh.read(7) # read 7 chars from the current file pointer

fh.tell()

fh.read() # read the entire file from the current file pointer

fh.tell()

fh.seek(4)  # move the file pointer to 4 

fh.read(15)

#======================================
#script 7  : file paths
#======================================
import sys
import os
import inspect

print( "Python " + sys.version )
print()

print( '__file__' )                                        
print( sys.argv[0] )                                  
print( inspect.stack()[0][1] )                         
print( sys.path[0] )                                    
print()

print( os.path.realpath(__file__) )                     
print( os.path.abspath(__file__) )                      
print( os.path.basename(__file__) )                      
print( os.path.basename(os.path.realpath(sys.argv[0])) )

