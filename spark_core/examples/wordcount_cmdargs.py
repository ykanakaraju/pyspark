# -*- coding: utf-8 -*-
"""
NOTE: Passing Command line arguments from Spyder
1. Run -> Configuration per file
2. Check "Command line options" check box
3. Enter the arguments in the text box that appears.
"""

import findspark
findspark.init()

from pyspark import SparkContext
import os
import shutil 
import sys

data_path = "C:\\PySpark\\data"

sc = SparkContext("local", "wordcount")
       
input_file = sys.argv[1]
output_dir = sys.argv[2]

#text_file = sc.textFile(os.path.join(data_path, "wordcount.txt") )
text_file = sc.textFile(os.path.join(data_path, input_file) )
        
wordcount = text_file.flatMap(lambda line: line.split(" "))  \
                .map(lambda word: (word, 1)) \
                .reduceByKey(lambda a, b: a + b)
    
#output_path = os.path.join(data_path, "wordcount_out")
output_path = os.path.join(data_path, output_dir)

# delete the output directory if exists..
if (os.path.isdir(output_path)):
    shutil.rmtree(output_path)

wordcount.saveAsTextFile(output_path)

sc.stop()