"""
The Imputer estimator completes missing values in a dataset, either 
using the mean or the median of the columns in which the missing 
values are located. The input columns should be of DoubleType or FloatType. 
Currently Imputer does not support categorical features and possibly 
creates incorrect values for columns containing categorical features. 

Imputer can impute custom values other than ‘NaN’ by .setMissingValue(custom_value). 
For example, .setMissingValue(0) will impute all occurrences of (0).

Note all null values in the input columns are treated as missing, 
and so are also imputed.
"""

import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.ml.feature import Imputer
from pyspark.sql import SparkSession

sc = SparkContext("local", "Transformer - Imputer")
spark = SparkSession(sc)
        
df = spark.createDataFrame([
    (12.0, float("nan")),
    (21.0, float("nan")),
    (float("nan"), 33.0),
    (42.0, 43.0),
    (15.0, 25.0)
], ["a", "b"])

imputer = Imputer(inputCols=["a", "b"], outputCols=["out_a", "out_b"], strategy="mean")
model = imputer.fit(df)
model.transform(df).show()

spark.stop()