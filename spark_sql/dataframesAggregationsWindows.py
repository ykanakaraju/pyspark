# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Basic Dataframe Operations") \
    .config("spark.master", "local") \
    .getOrCreate()

retails_data_csv = "E:\\PySpark\\data\\retail-data\\all\\*.csv"

df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load(retails_data_csv)\
  .repartition(5)
  
df.cache()

df.printSchema()

df.show(2, False, True)

df.count()

# ----------------------------------------------------------
#  Example 1 
# ----------------------------------------------------------
    
from pyspark.sql.window import Window
from pyspark.sql.functions import max, avg, col, dense_rank, rank, row_number, to_date


dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "M/d/yyyy H:mm"))

dfWithDate \
    .select("CustomerID", "date", "Quantity") \
    .where("CustomerID IS NOT NULL") \
    .orderBy("CustomerID", "date") \
    .show(100)

dfWithDate.printSchema()

windowSpec = Window \
  .partitionBy("CustomerID", "date") \
  .orderBy("Quantity") \
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
 
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
avgPurchaseQuantity = avg(col("Quantity")).over(windowSpec)
purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)
rowNumber = row_number().over(windowSpec)

dfWithDate \
  .where("CustomerId IS NOT NULL") \
  .orderBy("CustomerId")\
  .select(
    col("CustomerId"),
    col("date"),
    col("Quantity"),
    rowNumber.alias("rowNumber"),
    purchaseRank.alias("qtyRank"),
    purchaseDenseRank.alias("qtyDenseRank"),
    avgPurchaseQuantity.alias("avgQty"),
    maxPurchaseQuantity.alias("maxQty")).show(100)


spark.stop()