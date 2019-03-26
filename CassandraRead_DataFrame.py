import os
import sys

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.2 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'



#from pyspark import SparkContext
#sc = SparkContext("local", "movie lens app")

# Creating PySpark SQL Context
#from pyspark.sql import SQLContext
#sqlContext = SQLContext(sc)

#table_df = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="test_c", keyspace="mdm").load()
#table_df.show()

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("cassandra data read").getOrCreate()
table_df = spark.read.format("org.apache.spark.sql.cassandra").options(table="test_c", keyspace="mdm").load()
table_df.show()
print("List of columns ",table_df.columns)


