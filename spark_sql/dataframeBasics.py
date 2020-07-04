# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession

data_path = "C:\\PySpark\\data\\"

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Basics") \
    .config("spark.master", "local") \
    .getOrCreate()
    
'''
spark.conf.set("spark.executor.memory", '8g')
spark.conf.set('spark.executor.cores', '3')
spark.conf.set('spark.cores.max', '3')
spark.conf.set("spark.driver.memory",'8g')
'''

df = spark.read.json(data_path + "users.json")

df.show()     

# print the schema of the dataframe
df.printSchema()

# dataframe operations
df.select("name").show()

df.select(df['name'], df['age'] + 1).show()

df.filter(df['age'] > 20).show()
df.filter( (df['age'] > 20) | (df['age'].isNull()) ).show()

df.groupBy("age").count().show()
df.filter(df['age'].isNotNull()).groupBy("age").count().show()

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("users")

#sqlDF = spark.sql("SELECT * FROM users")
sqlDF = spark.sql("SELECT name, age+1 FROM users")
sqlDF.show()

# Register the DataFrame as a global temporary view
df.createGlobalTempView("gtv_users")

# Global temporary view is tied to a system preserved database `global_temp`
gtvDF = spark.sql("SELECT * FROM global_temp.gtv_users")
gtvDF.show()

spark2 = spark.newSession()
gtvDF2 = spark.newSession().sql("SELECT * FROM global_temp.gtv_users")
gtvDF2.show()

spark.stop()