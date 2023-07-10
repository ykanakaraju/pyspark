# -*- coding: utf-8 -*-
"""
Created on Tue Jan 29 16:53:55 2019

@author: Kanakaraju
"""
"""
NOTE: Passing Command line arguments from Spyder
1. Run -> Configuration per file
2. Check "Command line options" check box
3. Enter the arguments in the text box that appears.
"""

import sys

program_name = sys.argv[0]
arguments = sys.argv[1:]
count = len(arguments)
i = 0

for x in sys.argv:
     print ("Argument: ", i, x)
     i += 1
     
if len (sys.argv) < 2 :
    print ("Usage: python cmd-line-args.py <args>")
    sys.exit (1)
    
    