import os
import sys

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.2 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'

try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    print("Import successful")
except ImportError as e:
    print("Error during spark import ", e)
    sys.exit(1)


