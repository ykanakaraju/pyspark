# -*- coding: utf-8 -*-
"""
Created on Thu Aug 15 10:27:30 2019

@author: Kanakaraju Y
"""

import findspark
findspark.init()

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, OneHotEncoder, VectorAssembler, IndexToString
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

#SparkContext.setSystemProperty("hive.metastore.uris", "thrift://nn1:9083")
#spark = SparkSession.builder.enableHiveSupport().getOrCreate()

spark = SparkSession \
        .builder \
        .appName("Titanic - RandomForestClassifier") \
        .config("spark.master", "local") \
        .getOrCreate()
    
# spark is an existing SparkSession
ml_data_path = "C:\\PySpark\\data\\ml\\"    
train = spark.read.csv(ml_data_path + "train_titanic.csv", header = True)

# Displays the content of the DataFrame to stdout
train.show(10)

# String to float on some columns of the dataset : creates a new dataset
train = train.select(col("Survived"),col("Sex"),col("Embarked"),col("Pclass").cast("float"),col("Age").cast("float"),col("SibSp").cast("float"),col("Fare").cast("float"))

# dropping null values
train = train.dropna()

train.count()
train.printSchema()

# Spliting in train and test set. Beware : It sorts the dataset
(traindf, testdf) = train.randomSplit([0.7,0.3])

print("Training Data Count: ", traindf.count())
print("Testing Data Count: ", testdf.count())

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
genderIndexer = StringIndexer(inputCol="Sex", outputCol="indexedSex")
embarkIndexer = StringIndexer(inputCol="Embarked", outputCol="indexedEmbarked")
surviveIndexer = StringIndexer(inputCol="Survived", outputCol="indexedSurvived")

# One Hot Encoder on indexed features
genderEncoder = OneHotEncoder(inputCol="indexedSex", outputCol="sexVec")
embarkEncoder = OneHotEncoder(inputCol="indexedEmbarked", outputCol="embarkedVec")

# Create the vector structured data (label,features(vector))
assembler = VectorAssembler(inputCols=["Pclass","sexVec","Age","SibSp","Fare","embarkedVec"], outputCol="features")

# Train a RandomForest model.
rf = RandomForestClassifier(labelCol="indexedSurvived", featuresCol="features")

# Chain indexers and forest in a Pipeline
pipeline = Pipeline(stages=[surviveIndexer, genderIndexer, embarkIndexer, genderEncoder,embarkEncoder, assembler, rf]) 

# Train model.  This also runs the indexers.
traindf.printSchema()
model = pipeline.fit(traindf)
 
testdf.printSchema()
# Predictions
predictions = model.transform(testdf)
 
predictions.printSchema()

# Select example rows to display.
predictions.columns

# Select example rows to display.
predictions.select("prediction", "indexedSurvived", "features").show(50)

# Select (prediction, true label) and compute test error
predictions = predictions.select(col("Survived").cast("Float"),col("prediction"))

evaluator = MulticlassClassificationEvaluator(labelCol="Survived", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))

rfModel = model.stages[6]
print(rfModel)  # summary only

# model evaluation
evaluator = MulticlassClassificationEvaluator(labelCol="Survived", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy = %g" % accuracy)

evaluatorf1 = MulticlassClassificationEvaluator(labelCol="Survived", predictionCol="prediction", metricName="f1")
f1 = evaluatorf1.evaluate(predictions)
print("f1 = %g" % f1)

evaluatorwp = MulticlassClassificationEvaluator(labelCol="Survived", predictionCol="prediction", metricName="weightedPrecision")
wp = evaluatorwp.evaluate(predictions)
print("weightedPrecision = %g" % wp)

evaluatorwr = MulticlassClassificationEvaluator(labelCol="Survived", predictionCol="prediction", metricName="weightedRecall")
wr = evaluatorwr.evaluate(predictions)
print("weightedRecall = %g" % wr)

sc.stop()
