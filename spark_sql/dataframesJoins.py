# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Basic Dataframe Operations") \
    .config("spark.master", "local") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", '5')

employee = spark.createDataFrame([
    (1, "Raju", 25, 101),
    (2, "Ramesh", 26, 101),
    (3, "Amrita", 30, 102),
    (4, "Madhu", 32, 102),
    (5, "Aditya", 28, 102),
    (6, "Pranav", 28, 100)])\
  .toDF("id", "name", "age", "deptid")
  
employee.printSchema()
employee.show()  
  
department = spark.createDataFrame([
    (101, "IT", 1),
    (102, "ITES", 1),
    (103, "Opearation", 1),
    (104, "HRD", 2)])\
  .toDF("id", "deptname", "locationid")
  
department.show()  
department.printSchema()


employee.createOrReplaceTempView("employee")
department.createOrReplaceTempView("department")

spark.catalog.listTables()

joinEmpDept = employee["deptid"] == department["id"]

#------------------------
#   inner join
#------------------------
from pyspark.sql.functions import col

employee.join(department, joinEmpDept, "full_outer") \
        .drop(department["id"]) \
        .show()


#employee.join(department, joinEmpDept, "inner").show() 

sql_ij = """SELECT * FROM employee LEFT ANTI JOIN department
            ON employee.deptid = department.id"""
            
df_ij = spark.sql(sql_ij)
df_ij.show()

df_ij.rdd.getNumPartitions()


#------------------------
#   outer join
#------------------------
employee.join(department, joinEmpDept, "outer").show()
'''
sql_foj = """SELECT * FROM employee FULL OUTER JOIN department
               ON employee.deptid = department.id"""
spark.sql(sql_foj).show()
'''

#------------------------
#   left outer join
#------------------------
employee.join(department, joinEmpDept, "left_outer").show() 

#------------------------
#   right outer join
#------------------------
employee.join(department, joinEmpDept, "right_outer").show()

#------------------------
#  left semi join
#------------------------
employee.join(department, joinEmpDept, "left_semi").show()

#------------------------
#  left anti join
#------------------------
employee.join(department, joinEmpDept, "left_anti").show()

#------------------------
#  cross join
#------------------------
employee.join(department, joinEmpDept, "cross").show()
   
#------------------------
#  broadcast join
#------------------------
from pyspark.sql.functions import broadcast

joinExpr = employee["deptid"] == department['id']
bcDf = employee.join(broadcast(department), joinExpr, "left_outer")
bcDf.show()
bcDf.explain()

spark.stop()




