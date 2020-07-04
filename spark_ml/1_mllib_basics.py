## Classification Example

import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("Spark MLlib") \
        .config("spark.master", "local") \
        .getOrCreate()

data_simple_ml = "C:\\PySpark\\data\\simple-ml"
df = spark.read.json(data_simple_ml)

df.printSchema()
df.orderBy("value2").show()

''' 
RFormula produces a vector column of features and a double or string column of label.
 
In this case we want to use all available variables (the .) and also add in the 
interactions between value1 and color and value2 and color, treating those as new features
'''
     
from pyspark.ml.feature import RFormula
supervised = RFormula(formula="lab ~ . + color:value1 + color:value2")

fittedRF = supervised.fit(df)
preparedDF = fittedRF.transform(df)
preparedDF.show(truncate=False)

train, test = preparedDF.randomSplit([0.7, 0.3])

from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(labelCol="label", featuresCol="features")

print ( lr.explainParams() )

fittedLR = lr.fit(train)

train.printSchema()
test.printSchema()
#fittedLR.transform(train).select("label", "prediction").show()

dfLrTrain = fittedLR.transform(test)    

dfLrTrain.printSchema()

 
dfLrTrain.printSchema()    
dfLrTrain.select("label", "prediction").show(150)

# Persist the model


spark.stop()








