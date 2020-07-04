"""
StringIndexer encodes a string column of labels to a column of label indices. 

The indices are in [0, numLabels), and four ordering options are supported: 
“frequencyDesc” (default), “frequencyAsc”, “alphabetDesc”, “alphabetAsc” 

The unseen labels will be put at index numLabels if user chooses to keep them. 

If the input column is numeric, we cast it to string and index the string values. 

When downstream pipeline components such as Estimator or Transformer make use of 
this string-indexed label, you must set the input column of the component to this 
string-indexed column name. 

In many cases, you can set the input column with setInputCol.
"""

import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SQLContext, SparkSession

sc = SparkContext("local", "Features - StringIndexer")
sqlContext = SQLContext(sc)
spark = SparkSession.builder.getOrCreate()
        
df = spark.createDataFrame(
    [(0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")],
    ["id", "category"])

indexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
indexed = indexer.fit(df).transform(df)
indexed.show()

spark.stop()