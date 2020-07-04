# -*- coding: utf-8 -*-
"""
Created on Wed Aug 14 18:59:46 2019

@author: Kanakaraju
"""
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, VectorIndexer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
#import matplotlib.pyplot as plt

ml_data_path = "F:\\PySpark\\data\\ml\\"

spark = SparkSession \
    .builder \
    .appName("GBT Bike Sharing") \
    .config("spark.master", "local") \
    .getOrCreate()
    
df = spark \
    .read \
    .format('csv') \
    .option("header", 'true') \
    .load(ml_data_path + "bikeSharing.csv")

df.cache()
df.show()
df.printSchema()

print("Our dataset has %d rows." , df.count())

df = df.drop("instant").drop("dteday").drop("casual").drop("registered")
df.show()

df.printSchema()

df = df.select([col(c).cast("double").alias(c) for c in df.columns])
df.printSchema()

train, test = df.randomSplit([0.7, 0.3])

print("Training Data Count: ", train.count())
print("Testing Data Count: ", test.count())

#print("We have %d training examples and %d test examples." , (train.count(), test.count()))

featuresCols = df.columns
featuresCols.remove('cnt')
# This concatenates all feature columns into a single feature vector in a new column "rawFeatures".
vectorAssembler = VectorAssembler(inputCols=featuresCols, outputCol="rawFeatures")
# This identifies categorical features and indexes them.
vectorIndexer = VectorIndexer(inputCol="rawFeatures", outputCol="features", maxCategories=4)

display(train.select("hr", "cnt"))

gbt = GBTRegressor(labelCol="cnt")

# Define a grid of hyperparameters to test:
#  - maxDepth: max depth of each decision tree in the GBT ensemble
#  - maxIter: iterations, i.e., number of trees in each GBT ensemble
# In this example notebook, we keep these values small.  In practice, to get the highest accuracy, you would likely want to try deeper trees (10 or higher) and more trees in the ensemble (>100).
paramGrid = ParamGridBuilder().addGrid(gbt.maxDepth, [2, 5]).addGrid(gbt.maxIter, [10, 100]).build()
# We define an evaluation metric.  This tells CrossValidator how well we are doing by comparing the true labels with predictions.
evaluator = RegressionEvaluator(metricName="rmse", labelCol=gbt.getLabelCol(), predictionCol=gbt.getPredictionCol())
# Declare the CrossValidator, which runs model tuning for us.
cv = CrossValidator(estimator=gbt, evaluator=evaluator, estimatorParamMaps=paramGrid)

pipeline = Pipeline(stages=[vectorAssembler, vectorIndexer, cv])


# fit the training data to the pipeline... 
pipelineModel = pipeline.fit(train)

predictions = pipelineModel.transform(test)

display(predictions.select("cnt", "prediction", *featuresCols))
predictions.select("cnt", "prediction", *featuresCols).show()

rmse = evaluator.evaluate(predictions)
print("RMSE on our test set: %g" , rmse)

display(predictions.select("hr", "prediction"))
predictions.select("hr", "prediction").show()

display(predictions.select("hr", "prediction"))

#plt.show()







