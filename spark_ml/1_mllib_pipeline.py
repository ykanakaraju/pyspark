import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("Spark MLlib") \
        .config("spark.master", "local") \
        .getOrCreate()
        
data_simple_ml = "C:\\PySpark\\data\\simple-ml"
data_simple_ml_persist = "C:\\PySpark\\data\\simple-ml\\persisted-models"

df = spark.read.json(data_simple_ml)

df.printSchema()
df.orderBy("value2").show()

train, test = df.randomSplit([0.7, 0.3])

from pyspark.ml.feature import RFormula
from pyspark.ml.classification import LogisticRegression

rForm = RFormula()
lr = LogisticRegression().setLabelCol("label").setFeaturesCol("features")

from pyspark.ml import Pipeline

stages = [rForm, lr]
pipeline = Pipeline().setStages(stages)

from pyspark.ml.tuning import ParamGridBuilder

params = ParamGridBuilder()\
  .addGrid(rForm.formula, [
    "lab ~ . + color:value1",
    "lab ~ . + color:value1 + color:value2"])\
  .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])\
  .addGrid(lr.regParam, [0.1, 2.0])\
  .build()
  
from pyspark.ml.evaluation import BinaryClassificationEvaluator
evaluator = BinaryClassificationEvaluator()\
  .setMetricName("areaUnderROC")\
  .setRawPredictionCol("prediction")\
  .setLabelCol("label")
  
from pyspark.ml.tuning import TrainValidationSplit
tvs = TrainValidationSplit()\
  .setTrainRatio(0.75)\
  .setEstimatorParamMaps(params)\
  .setEstimator(pipeline)\
  .setEvaluator(evaluator)

tvsFitted = tvs.fit(train)

# get the predictions for the test data
dfTvsPredicts = tvsFitted.transform(test)
dfTvsPredicts.printSchema()
dfTvsPredicts.show(5)
    
    
metricAUC = evaluator.evaluate(tvsFitted.transform(test))
print(metricAUC)

#from pyspark.ml import PipelineModel
#from pyspark.ml.classification import LogisticRegressionModel
trainedPipeline = tvsFitted.bestModel
TrainedLR = trainedPipeline.stages[1]
summaryLR = TrainedLR.summary
summaryLR.objectiveHistory

spark.stop()  