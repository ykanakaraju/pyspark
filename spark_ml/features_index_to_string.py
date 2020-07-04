"""
Symmetrically to StringIndexer, IndexToString maps a column of label 
indices back to a column containing the original labels as strings. 

A common use case is to produce indices from labels with StringIndexer, 
train a model with those indices and retrieve the original labels from 
the column of predicted indices with IndexToString. However, you are 
free to supply your own labels.
"""
import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.ml.feature import IndexToString, StringIndexer
from pyspark.sql import SQLContext, SparkSession

sc = SparkContext("local", "Features - IndexToString")
sqlContext = SQLContext(sc)
spark = SparkSession.builder.getOrCreate()
        
df = spark.createDataFrame(
    [(0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")],
    ["id", "category"])

indexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
model = indexer.fit(df)
indexed = model.transform(df)

print("Transformed string column '%s' to indexed column '%s'"
      % (indexer.getInputCol(), indexer.getOutputCol()))

indexed.show()

print("StringIndexer will store labels in output column metadata\n")

converter = IndexToString(inputCol="categoryIndex", outputCol="originalCategory")

converted = converter.transform(indexed)

print("Transformed indexed column '%s' back to original string column '%s' using "
      "labels in metadata" % (converter.getInputCol(), converter.getOutputCol()))

converted.select("id", "categoryIndex", "originalCategory").show()

spark.stop()