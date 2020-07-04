# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession

retails_data_csv = "C:\\PySpark\\data\\retail-data\\all\\*.csv"

spark = SparkSession \
    .builder \
    .appName("Basic Dataframe Operations") \
    .config("spark.master", "local") \
    .getOrCreate()

'''
spark.conf.set("spark.executor.memory", '8g')
spark.conf.set('spark.executor.cores', '3')
spark.conf.set('spark.cores.max', '3')
spark.conf.set("spark.driver.memory",'8g')
'''
df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load(retails_data_csv)\
  .coalesce(5)
  
df.cache()

df.createOrReplaceTempView("dfTable")

df.printSchema()
# ----------------------------------------------------------
#  Example 1 
# ----------------------------------------------------------
    
from pyspark.sql.window import Window
from pyspark.sql.functions import desc

windowSpec = Window \
  .partitionBy("CustomerId", "date") \
  .orderBy("Quantity") \
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

from pyspark.sql.functions import max,col
maxPurchaseQuantity = min(col("Quantity")).over(windowSpec)

from pyspark.sql.functions import dense_rank, rank
purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)

from pyspark.sql.functions import col, to_date
dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")
spark.sql("SELECT InvoiceDate, date FROM dfWithDate").show()

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
  .select(
    col("CustomerId"),
    col("date"),
    col("Quantity"),
    purchaseRank.alias("quantityRank"),
    purchaseDenseRank.alias("quantityDenseRank"),
    maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()


spark.stop()