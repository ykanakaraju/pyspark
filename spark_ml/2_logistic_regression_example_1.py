import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("Spark MLlib") \
        .config("spark.master", "local") \
        .getOrCreate()

sc = spark.sparkContext

from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression

bdf = sc.parallelize([
    Row(label=1.0, weight=1.0, features=Vectors.dense(0.0, 5.0)),
    Row(label=0.0, weight=2.0, features=Vectors.dense(1.0, 2.0)),
    Row(label=1.0, weight=3.0, features=Vectors.dense(2.0, 1.0)),
    Row(label=0.0, weight=4.0, features=Vectors.dense(3.0, 3.0))]).toDF()

blor = LogisticRegression(regParam=0.01, weightCol="weight")
blorModel = blor.fit(bdf)
blorModel.coefficients
blorModel.intercept

test1 = sc.parallelize([Row(features=Vectors.sparse(2, [0], [1.0]))]).toDF()
blorModel.transform(test1).head().prediction

save_path = "C:\\PySpark\\spark_ml\\saved_models\\logistic_regression_example_1\\"
estimator_path = save_path + "lr"
# Save the estimator
blor.save(estimator_path)
lr2 = LogisticRegression.load(estimator_path)
lr2.getRegParam()

#save the model
model_path = save_path + "lr_model"
blorModel.save(model_path)

from pyspark.ml.classification import LogisticRegressionModel
model2 = LogisticRegressionModel.load(model_path)
print(blorModel.coefficients[0] == model2.coefficients[0])
print(blorModel.intercept == model2.intercept)
print(model2, blorModel)

spark.stop()