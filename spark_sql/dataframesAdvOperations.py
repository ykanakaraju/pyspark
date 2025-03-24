# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, column

flight_json_dir = "C:\\PySpark\\data\\flight-data\\json\\"
flight_json_2015 = flight_json_dir + "2015-summary.json"

spark = SparkSession \
    .builder \
    .appName("Basic Dataframe Operations") \
    .config("spark.master", "local") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", '5')

'''
spark.conf.set("spark.executor.memory", '8g')
spark.conf.set('spark.executor.cores', '3')
spark.conf.set('spark.cores.max', '3')
spark.conf.set("spark.driver.memory",'8g')
'''
######################################################    
#  partitionBy
######################################################   

#df = spark.read.format("json").load(flight_json_2015)
df = spark.read.json(flight_json_2015)

df.repartition(col("ORIGIN_COUNTRY_NAME"))\
       .write\
       .partitionBy("ORIGIN_COUNTRY_NAME")\
       .option("header", "true")\
       .mode("overwrite") \
       .csv("C:\\PySpark\\data\\partitionBy\\ex1")


df.repartition(col("ORIGIN_COUNTRY_NAME"), col("DEST_COUNTRY_NAME"))\
       .write\
       .partitionBy("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")\
       .option("header", "true")\
       .csv("C:\\PySpark\\data\\partitionBy\\ex2")


######################################################    
#  storing in persistent tables - saveAsTable
######################################################  
  
# this table will be materialized at the default hive metahouse
# in this case the location of this is : C:\Users\kanak\spark-warehouse
       
df.write \
  .mode("overwrite") \
  .partitionBy("ORIGIN_COUNTRY_NAME") \
  .saveAsTable("flights_table_1")    
       
df2 = spark.sql("select * from flights_table_1")       
df2.printSchema()
df2.show()


######################################################    
#  bucketBy & saveAsTable
######################################################  

# warehouse location: C:\Users\kanak\spark-warehouse

df.write\
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .bucketBy(4, "ORIGIN_COUNTRY_NAME")\
    .sortBy("ORIGIN_COUNTRY_NAME")\
    .saveAsTable("flights_bucketed_csv")

df3 = spark.sql("select * from flights_bucketed_csv")
df3.printSchema()
df3.show() 


    
# Stop the sparksession
spark.stop()