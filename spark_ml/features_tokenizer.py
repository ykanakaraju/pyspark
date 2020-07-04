import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
from pyspark.sql import SQLContext, SparkSession

sc = SparkContext("local", "Features - Tokenizer")
sqlContext = SQLContext(sc)
spark = SparkSession.builder.getOrCreate()
        
df = spark.createDataFrame([
        (0, "Sample Tokenizer Sentence"),
        (1, "Pyhon is a cool langage"),
        (2, "Comma,Separated,Words")
    ], ["id", "sentence"])

countTokens = udf(lambda words: len(words), IntegerType())

# Tokenizer
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
tokenized = tokenizer.transform(df)
tokenized.select("sentence", "words")\
    .withColumn("tokens", countTokens(col("words")))\
    .show(truncate=False)

# RegexTokenizer
regexTokenizer = RegexTokenizer(inputCol="sentence", outputCol="words", pattern="\\w+", gaps=False)
# regexTokenizer = RegexTokenizer(inputCol="sentence", outputCol="words", pattern="\\W")
regexTokenized = regexTokenizer.transform(df)
regexTokenized.select("sentence", "words") \
    .withColumn("tokens", countTokens(col("words")))\
    .show(truncate=False)