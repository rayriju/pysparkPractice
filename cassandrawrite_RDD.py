import os
import sys
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.2 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'


try:
    from pyspark import SparkConf,SparkContext
    from pyspark.sql import SparkSession
    print("Spark Import successful")
except ImportError as e:
    print("Spark imports unsuccessful ", e)
    sys.exit(1)

#conf = SparkConf.setAppName("File read").setMaster("local")
#sc = SparkContext(conf=conf)

csv_file_path = '/home/riju/PycharmProjects/cassandaconn/newdata.csv'
spark = SparkSession.builder.getOrCreate()

try:
    data = spark.read.csv(path=csv_file_path,header=True)
    data.show()
except FileNotFoundError as e:
    print("File not available at path ", e)

data.write.format("org.apache.spark.sql.cassandra").options(table="test_c", keyspace="mdm").save(mode="append")
print( "{0} records inserted successfully " , data.count())
