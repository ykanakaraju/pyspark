# -*- coding: utf-8 -*-
import findspark
findspark.init()

from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark.sql import Row

warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Datasorces") \
    .config("spark.master", "local") \
    .config("spark.sql.warehouse.dir", warehouse_location)\
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
spark.sql("LOAD DATA LOCAL INPATH 'C:/PySpark/data/hive/kv1.txt' INTO TABLE src")

spark.sql("SELECT * FROM src").show()

spark.sql("SELECT COUNT(*) FROM src").show()

sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

stringsDS = sqlDF.rdd.map(lambda row: "Key: %d, Value: %s" % (row.key, row.value))

for record in stringsDS.collect():
    print(record)
    
Record = Row("key", "value")
recordsDF = spark.createDataFrame([Record(i, "val_" + str(i)) for i in range(1, 101)])
recordsDF.createOrReplaceTempView("records")

spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()

spark.stop()