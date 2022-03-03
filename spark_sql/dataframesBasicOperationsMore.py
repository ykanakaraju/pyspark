# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, udf, col
from pyspark.sql.types import StringType

spark = SparkSession \
        .builder \
        .appName("Dataframe Operations") \
        .config("spark.master", "local[*]") \
        .getOrCreate()   
        
spark.conf.set("spark.sql.shuffle.partitions", "10")


listUsers = [(1, "Raju", 5),
             (2, "Ramesh", 15),
             (3, "Rajesh", 18),
             (4, "Raghu", 35),
             (5, "Ramya", 25),
             (6, "Radhika", 35),
             (7, "Ravi", 70)]

userDf = spark.createDataFrame(listUsers, ["id", "name", "age"])
userDf.show()
userDf.printSchema()

######################################################    
#  withColumn with 'when' operator
######################################################

users2 = userDf.withColumn("ageGroup", 
                           when(userDf["age"] <= 12, "child")
                           .when(userDf["age"] < 20, "teenager")
                           .when(userDf["age"] < 60, "adult")
                           .otherwise("senior"))
users2.show()


######################################################    
#  dropDuplicates
######################################################

listUsers = [(1, "Raju", 5),
             (1, "Raju", 5),
             (3, "Raju", 5),
             (4, "Raghu", 35),
             (4, "Raghu", 35),
             (6, "Raghu", 35),
             (7, "Ravi", 70)]

userDf = spark.createDataFrame(listUsers, ["id", "name", "age"])
userDf.show()

userDf.dropDuplicates().show()
userDf.dropDuplicates(['name', 'age']).show()

spark.stop() 

######################################################    
#  dropna - drop nulls
######################################################

usersDf = spark.read.json("E:\\PySpark\\data\\users.json")
usersDf.show()

usersDf.dropna().show()
usersDf.dropna(subset=['phone', 'age']).show()

######################################################    
#  udf - user defined function
######################################################

def getAgeGroup( age ):
    if (age <= 12):
        return "child"
    elif (age >= 13 and age <= 19):
        return "teenager"
    elif (age >= 20 and age < 60):
        return "adult"
    else:
        return "senior"
    
getAgeGroupUDF = udf(getAgeGroup, StringType())

usersDf = spark.createDataFrame([(1, "Raju", 5),
             (2, "Ramesh", 15),
             (3, "Rajesh", 18),
             (4, "Raghu", 35),
             (5, "Ramya", 25),
             (6, "Radhika", 35),
             (7, "Ravi", 70)], ["id", "name", "age"])

usersDf.show()

#usersDf2 = usersDf.withColumn("ageGroup", getAgeGroupUDF(usersDf["age"]))
usersDf2 = usersDf.withColumn("ageGroup", getAgeGroupUDF(col("age")))
usersDf2.show()

# Using UDF in a SQL statement
spark.udf.register("getAgeGroupUDF", getAgeGroup, StringType())
usersDf.createOrReplaceTempView("users")
qry = "select id, name, age, getAgeGroupUDF(age) as ageGroup from users"
spark.sql(qry).show(truncate=False)
     
######################################################    
#  udf using annotation - user defined function
######################################################

@udf(returnType=StringType()) 
def getAgeGroupAnnot( age ):
    if (age <= 12):
        return "child"
    elif (age >= 13 and age <= 19):
        return "teenager"
    elif (age >= 20 and age < 60):
        return "adult"
    else:
        return "senior"

usersDf3 = usersDf.withColumn("ageGroup", getAgeGroupAnnot(col("age")))
usersDf2.show()







