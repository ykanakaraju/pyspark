# -*- coding: utf-8 -*-
import findspark
findspark.init()

#######  PREREQUITE ###########
# pyarrow package needs to be installed (pip install pyarrow)

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Basics") \
    .config("spark.master", "local") \
    .getOrCreate()
    
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
   
filePath = "C:\\PySpark\\data\\flight-data\\csv\\2015-summary.csv"
csv_data = pd.read_csv(filePath)
print(csv_data.head(4))

#convert pandas dataframe to spark dataframe
df = spark.createDataFrame(csv_data)
df.show()
df.printSchema()


#convert spark dataframe to pandas dataframe
pdf = df.select("*").toPandas()
print(pdf.head(10))

spark.stop()