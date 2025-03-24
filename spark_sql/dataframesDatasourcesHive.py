# -*- coding: utf-8 -*-
import findspark
findspark.init()

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, avg, count

warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Datasorces") \
    .config("spark.master", "local") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()
    
spark.catalog.listTables()  

spark.catalog.currentDatabase()
spark.catalog.listDatabases()

spark.sql("show databases").show()

spark.sql("drop database if exists sparkdemo cascade")
spark.sql("create database if not exists sparkdemo")
spark.sql("use sparkdemo")

spark.catalog.listTables()
spark.catalog.currentDatabase()

spark.sql("DROP TABLE IF EXISTS movies")
spark.sql("DROP TABLE IF EXISTS ratings")
spark.sql("DROP TABLE IF EXISTS topRatedMovies")
    
createMovies = """CREATE TABLE IF NOT EXISTS 
         movies (movieId INT, title STRING, genres STRING) 
         ROW FORMAT DELIMITED 
         FIELDS TERMINATED BY ','"""
    
loadMovies = """LOAD DATA LOCAL INPATH 'E:/PySpark/data/movielens/moviesNoHeader.csv' 
         OVERWRITE INTO TABLE movies"""
    
createRatings = """CREATE TABLE IF NOT EXISTS 
         ratings (userId INT, movieId INT, rating DOUBLE, timestamp LONG) 
         ROW FORMAT DELIMITED 
         FIELDS TERMINATED BY ','"""
    
loadRatings = """LOAD DATA LOCAL INPATH 'E:/PySpark/data/movielens/ratingsNoHeader.csv' 
         OVERWRITE INTO TABLE ratings"""
         
spark.sql(createMovies)
spark.sql(loadMovies)
spark.sql(createRatings)
spark.sql(loadRatings)
    
spark.catalog.listTables()
     
#Queries are expressed in HiveQL

moviesDF = spark.sql("SELECT * FROM movies")
ratingsDF = spark.sql("SELECT * FROM ratings")

moviesDF.show()
ratingsDF.show()
           
summaryDf = ratingsDF \
            .groupBy("movieId") \
            .agg(count("rating").alias("ratingCount"), avg("rating").alias("ratingAvg")) \
            .filter("ratingCount > 25") \
            .orderBy(desc("ratingAvg")) \
            .limit(10)
              
summaryDf.show()
    
joinStr = summaryDf.movieId == moviesDF.movieId
    
summaryDf2 = summaryDf.join(moviesDF, joinStr) \
                .drop(summaryDf["movieId"]) \
                .select("movieId", "title", "ratingCount", "ratingAvg") \
                .orderBy(desc("ratingAvg")) \
                .coalesce(1)
    
summaryDf2.show()
 
summaryDf2.write \
   .mode("overwrite") \
   .format("hive") \
   .saveAsTable("topRatedMovies")
   
spark.catalog.listTables()
        
topRatedMovies = spark.table("topRatedMovies")
topRatedMovies.show() 
    
spark.catalog.listFunctions()  
  
spark.stop()
