# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Basic Dataframe Operations") \
    .config("spark.master", "local") \
    .getOrCreate()

employee = spark.createDataFrame([
    (1, "Raju", 25, 101),
    (2, "Ramesh", 26, 101),
    (3, "Amrita", 30, 102),
    (4, "Madhu", 32, 102),
    (5, "Aditya", 28, 102),
    (6, "Pranav", 28, 10000)])\
  .toDF("id", "name", "age", "deptid")
  
department = spark.createDataFrame([
    (101, "IT", 1),
    (102, "Opearation", 1),
    (103, "HRD", 2)])\
  .toDF("id", "deptname", "locationid")

employee.createOrReplaceTempView("employee")
department.createOrReplaceTempView("department")

joinEmpDept = employee["deptid"] == department['id']

#------------------------
#   inner join
#------------------------
employee.join(department, joinEmpDept).show() 
#employee.join(department, joinEmpDept, "inner").show() 

sql_ij = """SELECT * FROM employee JOIN department
            ON employee.deptid = department.id"""
            
spark.sql(sql_ij).show()

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
bcDf = employee.join(broadcast(department), joinExpr, "outer")
bcDf.show()
bcDf.explain()

spark.stop()