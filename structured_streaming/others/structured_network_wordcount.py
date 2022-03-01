
from __future__ import print_function

import sys
import os

# setup the environment for the IDE
os.environ['SPARK_HOME'] = '/home/kanak/spark-2.4.7-bin-hadoop2.7'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python'

sys.path.append('/home/kanak/spark-2.4.7-bin-hadoop2.7/python')
sys.path.append('/home/kanak/spark-2.4.7-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip')

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: structured_network_wordcount.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

    host = "localhost"
    port = 9999

    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount")\
        .getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to host:port
    lines = spark\
        .readStream\
        .format('socket')\
        .option('host', host)\
        .option('port', port)\
        .load()

    # Split the lines into words
    words = lines.select(
        # explode turns each item in an array into a separate row
        explode(
            split(lines.value, ' ')
        ).alias('word')
    )

    # Generate running word count
    wordCounts = words.groupBy('word').count()

    # Start running the query that prints the running counts to the console
    query = wordCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()

    query.awaitTermination()
