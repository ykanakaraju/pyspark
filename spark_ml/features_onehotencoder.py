import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql import SQLContext, SparkSession

sc = SparkContext("local", "Features - OneHotEncoder")
spark = SparkSession(sc)
        
df = spark.createDataFrame([
    (0, "a"),
    (1, "b"),
    (2, "c"),
    (3, "a"),
    (4, "a"),
    (5, "c")
], ["id", "category"])

stringIndexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
model = stringIndexer.fit(df)
indexed = model.transform(df)

encoder = OneHotEncoder(inputCol="categoryIndex", outputCol="categoryVec")
#encoder.setDropLast(False)
encoded = encoder.transform(indexed)
encoded.show()

spark.stop()