# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import StorageLevel

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
  .repartition(5)
  
df.rdd.getNumPartitions()

df.cache()  #df.persist(StorageLevel.MEMORY_ONLY)

df.createOrReplaceTempView("dfTable")

df.count()
df.show()
df.printSchema()
# ----------------------------------------------------------
#  Example 1 - count, countDistinct & approx_count_distinct
# ----------------------------------------------------------    
from pyspark.sql.functions import count, countDistinct, approx_count_distinct
df.select(count("StockCode")).show()
#df.groupBy("StockCode").count().show()
df.select(countDistinct("StockCode")).show()
df.select(approx_count_distinct("StockCode", 0.01)).show()

spark.sql("""SELECT count(StockCode),
                    approx_count_distinct(StockCode)
             FROM dfTable""").show()

# ----------------------------------------------------------
#  Example 2 - first, last, min, max
# ----------------------------------------------------------
from pyspark.sql.functions import first, last, min, max
df.select(first("StockCode"), last("StockCode")).show()
df.select(min("Quantity"), max("Quantity")).show()

spark.sql("""SELECT first(StockCode) as first, 
                    last(StockCode) as last,
                    min(Quantity)  as minQty,
                    max(Quantity) as maxQty
             FROM dfTable""").show()


# ----------------------------------------------------------
#  Example 3 - sum, sumDistinct, avg
# ----------------------------------------------------------
from pyspark.sql.functions import sum, sumDistinct, avg

df.select(sum("Quantity")).show() 
df.select(sumDistinct("Quantity")).show()
df.select(avg("Quantity")).show()

spark.sql("""SELECT sum(Quantity)  as sumQty, 
                    sum(DISTINCT Quantity) as sumDistinct,
                    mean(Quantity) as mean
             FROM dfTable""").show() 

from pyspark.sql.functions import mean, expr   
          
df.select(
  count("Quantity").alias("total_transactions"),
  sum("Quantity").alias("total_purchases"),
  avg("Quantity").alias("avg_purchases"),
  expr("mean(Quantity)").alias("mean_purchases")) \
.selectExpr(
  "total_purchases/total_transactions",
  "avg_purchases",
  "mean_purchases").show()

# ----------------------------------------------------------
# Example 4 - varience and standard deviation
#----------------------------------------------------------
                 
from pyspark.sql.functions import var_pop, stddev_pop, variance, stddev
from pyspark.sql.functions import var_samp, stddev_samp

df.select(variance("Quantity"), stddev("Quantity"),
  var_pop("Quantity"), var_samp("Quantity"),
  stddev_pop("Quantity"), stddev_samp("Quantity")).show()
    
spark.sql("""SELECT var_pop(Quantity), 
                    var_samp(Quantity),
                    stddev_pop(Quantity), 
                    stddev_samp(Quantity)
             FROM dfTable""").show()

#----------------------------------------------------------
# Example 5 - skewness & kurtosis
#----------------------------------------------------------
from pyspark.sql.functions import skewness, kurtosis
df.select(skewness("Quantity"), kurtosis("Quantity")).show()             

spark.sql("SELECT skewness(Quantity), kurtosis(Quantity) FROM dfTable").show() 

#----------------------------------------------------------
# Example 6 - Covariance and Correlation
#----------------------------------------------------------
from pyspark.sql.functions import corr, covar_pop, covar_samp

df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
    covar_pop("InvoiceNo", "Quantity")).show()

spark.sql("""SELECT corr(InvoiceNo, Quantity), 
                        covar_samp(InvoiceNo, Quantity),
                        covar_pop(InvoiceNo, Quantity)
                  FROM dfTable""").show()

#----------------------------------------------------------
#- Example 7 - Aggregation on Collections
#----------------------------------------------------------
from pyspark.sql.functions import collect_set, collect_list

df.count()

df.agg(collect_set("Country"), collect_list("Country")).show()

spark.sql("SELECT collect_set(Country), collect_list(Country) FROM dfTable") \
     .show()

#----------------------------------------------------------
#  Example 8 - Grouping
#----------------------------------------------------------
from pyspark.sql.functions import count, sum

df.groupBy("InvoiceNo", "CustomerId").sum("Quantity").show()

df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)"))\
   .show()

df.groupBy("InvoiceNo").agg(
    expr("avg(Quantity)"),expr("sum(Quantity)"),expr("stddev_pop(Quantity)"))\
  .show()
  
from pyspark.sql.functions import col, to_date
dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")
spark.sql("SELECT InvoiceDate, date FROM dfWithDate").show()



spark.stop()