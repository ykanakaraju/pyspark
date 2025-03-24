# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession

flight_json_dir = "C:\\PySpark\\data\\flight-data\\json\\"
flight_json_2015 = flight_json_dir + "2015-summary.json"

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

######################################################  
# Enforcing custom schema on a dataframe
######################################################  

from pyspark.sql.types import StructField, StructType, StringType, LongType

mySchema = StructType([
  StructField("DEST_COUNTRY_NAME", StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
  StructField("count", LongType(), False, metadata={"hello":"world"})
])

df2 = spark.read.format("json").schema(mySchema).load(flight_json_2015)
print( df2.schema )
print("\n\n")
df2.printSchema()

######################################################  
# Manually Creating Dataframes
######################################################  
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

customSchema = StructType([
  StructField("name", StringType(), True),
  StructField("age", IntegerType(), True),
  StructField("city", StringType(), False)
])

row1 = Row("Raju", 40, "Hyderabad")
row2 = Row("Ravi", 30, "Bangalore")
row3 = Row("Vijay", 25, "Chennai")

myDf = spark.createDataFrame([row1, row2, row3], customSchema)

myDf.show()



# Stop the sparksession
spark.stop()










