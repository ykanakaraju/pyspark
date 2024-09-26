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

#df = spark.read.json(data_path + "users.json")
df = spark.read.format("json").load(data_path + "users.json")

df.show()     

print( df.collect() )

# print the schema of the dataframe
df.printSchema()

print( df.schema )

# dataframe operations
df.select("*").show()

df2 = df.select("name", df["age"])
df2.show()


df.select(df['name'], df['age'] + 1).show()

df.filter(df['age'] > 20).show()
df.where("age > 20").show()
df.filter( (df['age'] > 20) | (df['age'].isNull()) ).show()

df.groupBy("age").count().show()

df.filter(df['age'].isNotNull()).groupBy("age").count().show()



# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("users")

spark.catalog.listTables()
#spark.catalog.dropTempView("users")


qry1 = "SELECT age, count(*) as count FROM users where age is not null group by age order by age limit 3"

#sqlDF = spark.sql("SELECT * FROM users")
sqlDF = spark.sql(qry1)
sqlDF.show()

spark2 = spark.newSession()

sqlDF2 = spark2.sql("SELECT name, age FROM users")
sqlDF2.show()

spark2.catalog.listTables()

# Register the DataFrame as a global temporary view
df.createGlobalTempView("gtv_users")

spark.catalog.listTables()

# Global temporary view is tied to a system preserved database `global_temp`
gtvDF = spark.sql("SELECT * FROM global_temp.gtv_users")
gtvDF.show()

gtvDF2 = spark2.sql("SELECT * FROM global_temp.gtv_users")
gtvDF2.show()

#########################################
# Storing to materialized tables
#########################################

df.write.format("csv").option("header", "true").mode("overwrite").saveAsTable("users_table")

df2 = spark.sql("select * from users_table where age is not null")
df2.printSchema()
df2.show()



spark.stop()