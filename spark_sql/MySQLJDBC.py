#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 22 15:05:27 2020

@author: kanakaraju
"""
import os
import sys

# setup the environment for the IDE
os.environ['SPARK_HOME'] = '/home/kanak/spark-2.4.7-bin-hadoop2.7'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python'

sys.path.append('/home/kanak/spark-2.4.7-bin-hadoop2.7/python')
sys.path.append('/home/kanak/spark-2.4.7-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip')

from pyspark.sql import SparkSession

spark = SparkSession.builder \
            .appName("JDBC_MySQL") \
            .config('spark.master', 'local') \
            .getOrCreate()
  
qry = "(select emp.*, dept.deptname from emp left outer join dept on emp.deptid = dept.deptid) emp"
                  
df_mysql = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost/sparkdb?autoReConnect=true&useSSL=false") \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("dbtable", qry)  \
            .option("user", "root") \
            .option("password", "kanakaraju") \
            .load()
                
df_mysql.show()  

spark.catalog.listTables()

df2 = df_mysql.filter("age > 30").select("id", "name", "age", "deptid")

df2.show()   

df2.printSchema()    

df2.write.format("jdbc") \
    .option("url", "jdbc:mysql://localhost/sparkdb?autoReConnect=true&useSSL=false") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("dbtable", "emp2")  \
    .option("user", "root") \
    .option("password", "kanakaraju") \
    .mode("overwrite") \
    .save()      
                                     
spark.stop()















