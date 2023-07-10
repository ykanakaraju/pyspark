# -*- coding: utf-8 -*-

#============================================= 
# While Loop
#============================================= 
times = 8

while (times > 0):
    print ( "*" * times )
    times -= 1
    
#=============================================    
# for Loop - example 1 - sum
#============================================= 
numbers = [6, 5, 3, 8, 4, 2, 5, 4, 11]
sum = 0

for val in numbers:
	sum = sum + val

print("The sum is", sum)

#============================================= 
# for Loop - example 2 - using range
#============================================= 
for i in range(2, 50, 3):
    if (i > 30):
        break
    
    # optional end parameter can be specified to replace the default new-line
    # character so that all output can be printed on the same line.
    print(i, end=' ')
else:
    print("Done!")
    
    
for i in range(2, 20, 3):
    print(i, end=' ')
else:
    print("Done!")    
    
#============================================= 
# range 
#============================================= 
print( range(10) )
print( list(range(10)) )
print( list(range(2, 8)) )
print( list(range(2, 20, 3)) )	

#============================================= 
# Nested Loops
#============================================= 
count = 0
for i in range(1, 8) :
    print (str(i) * i, end=" >> ")
    
    for j in range(0, i):
        count += 1
    
    print (count)
    
    
#============================================= 
# Loop Control Statements - break & continue
#=============================================  
    
for i in range (1, 20) :
   print (i, end = " ")
   if (i == 5) :
       break
print ('\nloop ends')

#--------------------------------

for i in range (1, 10) :
   
   if (i % 2 == 0) :
       continue
   else:
       print (i, end = " ")
print ('\nloop ends')

#--------------------------------

for k in range(1, 10):
    pass
print ('\nloop ends')