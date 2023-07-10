# -*- coding: utf-8 -*-
"""
Created on Sat Feb 23 16:39:15 2019

@author: Kanakaraju
"""

######################################################
'''
# Script 1: re module - regular expressions
'''
import re

txt = "Twinkle Twinkle Little Star"

x = re.search("^Twinkle.*Star$", txt)
  
print ("YES! We have a match!" if (x) else "No Match")

print( re.findall("le", txt) )

print( re.findall("how", txt) )

print( re.split("\s", txt) )

print( re.sub("\s", "##", txt) )
#replace first two occurances only
print( re.sub("\s", "##", txt, 2) )
              
a = re.search(r"\bLi\w+", txt)
print(a.span(), a.string, a.group(), sep=" | ")

######################################################
'''
# Script 2: Pattern Searching
'''

import re

patterns = ['Johny', 'papa', 'yes', 'johny']
text = """Johny Johny yes papa.
Eating sugar no papa.
Telling lies no papa.
Open your mouth.
Ha!
Ha!
Ha!"""

for p in patterns:
    print("Searching for '%s' in the rhyme: " % p, end = " ")
    if (re.search(p, text)):
        n = re.findall(p, text)
        print("%d matches found" % len(n))
    else:
        print("no matches found")


######################################################
'''
# Script 3: Pattern Searching
'''
import re

emails_str = """raju's email is raju@gmail.com, 
rahul's is rahul@yahoo.com, 
and few others are naren@gmail.com, hari@yahoo.com
"""

# findall() searches for the Regular Expression and return a list upon finding
emails = re.findall(r'[\w\.-]+@[\w\.-]+', emails_str)

for email in emails:
    print(email)
    
######################################################
'''
# Script 4: Pattern Searching

If we were going to use this regex repeatedly,we could compile it 
once and then use the compiled regex whenever we needed it
'''
import re 
  
# compile() creates regular expression character class [a-e]
p = re.compile('[a-e]')    
print(p.findall("Hello! How are you doing?")) 
  
# \d is equivalent to [0-9]. 
q = re.compile('\d') 
print(q.findall("11 A.M. on 8th Sept 2005")) 
  
# \d+ will match a group on [0-9], group of one or greater size 
r = re.compile('\d+') 
print(r.findall("11 A.M. on 8th Sept 2005")) 
    