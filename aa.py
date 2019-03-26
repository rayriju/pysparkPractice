py
help
Ray, Riju(Cognizant)
Fri
3 / 22 / 2019
6: 18
PM

-------oracle
connect
from pyspark

------------

/ spark - 2.1
.0 - bin - hadoop2
.7 / bin / pyspark

--jars
"/home/jars/ojdbc6.jar"

--master
yarn - client

--num - executors
10

--driver - memory
16
g

--executor - memory
8
g

empDF = spark.read \
 \
    .format("jdbc") \
 \
    .option("url", "jdbc:oracle:thin:username/password@//hostname:portnumber/SID") \
 \
    .option("dbtable", "hr.emp") \
 \
    .option("user", "db_user_name") \
 \
    .option("password", "password") \
 \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
 \
    .load()

---------cassandra
connect
from pyspark

------------

# Configuratins related to Cassandra connector & Cluster

import os

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'

# Configuratins related to Cassandra connector & Cluster

import os

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=192.168.0.123,192.168.0.124 pyspark-shell'

# Creating PySpark Context

from pyspark import SparkContext

sc = SparkContext("local", "movie lens app")

# Creating PySpark SQL Context

from pyspark.sql import SQLContext

sqlContext = SQLContext(sc)


# Loads and returns data frame for a table including key space given

def load_and_get_table_df(keys_space_name, table_name):
    table_df = sqlContext.read \
 \
        .format("org.apache.spark.sql.cassandra") \
 \
        .options(table=table_name, keyspace=keys_space_name) \
 \
        .load()


return table_df

# Loading movies & ratings table data frames

movies = load_and_get_table_df("movie_lens", "movies")

ratings = load_and_get_table_df("movie_lens", "ratings")

---------db2
connect
from pyspark

------------

from pyspark.sql import SQLContext

user = "<username>"  # for example, "adbcdefg"

password = "<password>"  # for example, "abdcEFGhijk"

jdbcURL = "jdbc:db2://<database host>:<port>/<database_name>"

# for example,  "jdbc:db2://99.999.99.999:9999/DM4"

prop = {"user": user, "password": password, "driver": "com.ibm.db2.jcc.DB2Driver",

        "sslConnection": "<true or false>"}

table = "<table_name>"  # for example: "DatamartTest"

data_from_datamart = sqlContext.read.jdbc(url=jdbcURL, table=table, properties=prop)

data_from_datamart.show()

== another

df = (sqlContext.read.format('jdbc').option('url',

                                            'jdbc:db2://xx.xx.xxx.xxx:50000/myDatabase').option('driver',

                                                                                                'com.ibm.db2.jcc.DB2Driver').option(
    'dbtable', "(SELECT * FROM mySchema.myTable

      limit 100) as t
") .option('user', user).option('password', password).load())

df.count()

- -------------AWS
S3a
connect
from pyspark

--------------------

from pyspark import SparkContext

from pyspark.sql import SparkSession

from pyspark import SparkConf

conf = (SparkConf().set(
“spark.executor.extraJavaOptions”, ”-Dcom.amazonaws.services.s3.enableV4=true”).set(“spark.driver.extraJavaOptions”, ”-Dcom.amazonaws.services.s3.enableV4 = true”))

scT = SparkContext(conf=conf)

scT.setSystemProperty(“com.amazonaws.services.s3.enableV4”, “true”)



hadoopConf = scT._jsc.hadoopConfiguration()

hadoopConf.set(“fs.s3a.awsAccessKeyId”, “XXXXX”)

hadoopConf.set(“fs.s3a.awsSecretAccessKey”, “XXXXX”)

hadoopConf.set(“fs.s3a.endpoint”, “s3 - ap - south - 1.
amazonaws.com”)

hadoopConf.set(“com.amazonaws.services.s3a.enableV4”, “true”)

hadoopConf.set(“fs.s3a.impl”, “org.apache.hadoop.fs.s3a.S3AFileSystem”)



sql = SparkSession(scT)

csv_df = sql.read.csv(‘s3a: // bucket / file.csv’)

== =another

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "...")

sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "...")

- --------------AWS
redshift
connect
from pyspark

-----------------------

download->

spark - redshift_2
.10 - 3.0
.0 - preview1.jar

RedshiftJDBC41 - 1.1
.10
.1010.jar

hadoop - aws - 2.7
.1.jar

aws - java - sdk - 1.7
.4.jar

(aws - java - sdk - s3 - 1.11
.60.jar) (newer version but not everything worked with it)

Put
these
jar
files in $SPARK_HOME / jars / and then
start
spark

pyspark - -jars $SPARK_HOME / jars / spark - redshift_2
.10 - 3.0
.0 - preview1.jar,$SPARK_HOME / jars / RedshiftJDBC41 - 1.1
.10
.1010.jar,$SPARK_HOME / jars / hadoop - aws - 2.7
.1.jar,$SPARK_HOME / jars / aws - java - sdk - s3 - 1.11
.60.jar,$SPARK_HOME / jars / aws - java - sdk - 1.7
.4.jar

(SPARK_HOME
should
be = "/usr/local/Cellar/apache-spark/$SPARK_VERSION/libexec")



This
will
run
Spark
with all necessary dependencies.Note that you also need to specify the authentication type 'forward_spark_s3_credentials'=True if you are using awsAccessKeys.

from pyspark.sql import SQLContext

from pyspark import SparkContext



sc = SparkContext(appName="Connect Spark with Redshift")

sql_context = SQLContext(sc)

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", < ACCESSID > )

sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", < ACCESSKEY > )



df = sql_context.read \
 \
        .format("com.databricks.spark.redshift") \
 \
        .option("url", "jdbc:redshift://example.coyf2i236wts.eu-central-    1.redshift.amazonaws.com:5439/agcdb?user=user&password=pwd") \
 \
        .option("dbtable", "table_name") \
 \
        .option('forward_spark_s3_credentials', True) \
 \
        .option("tempdir", "s3n://bucket") \
 \
        .load()

Common errors afterwards are:



    Redshift
Connection
Error: "SSL off"

Solution:.option("url",
                 "jdbc:redshift://example.coyf2i236wts.eu-central-    1.redshift.amazonaws.com:5439/agcdb?user=user&password=pwd?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory")

S3
Error: When
unloading
the
data, e.g.after
df.show()
you
get
the
message: "The bucket you are attempting to access must be addressed using the specified endpoint. Please send all future requests to this endpoint."

Solution: The
bucket & cluster
must
be
run
within
the
same
region

https: // stackoverflow.com / questions / 31395743 / how - to - connect - to - amazon - redshift - or -other - dbs - in -apache - spark

Esteem,

Riju
Ray

AIM – MDM | Cognizant
Technology
Solutions | (VNET 306166 | *: riju.ray2 @ cognizant.com
Ray, Riju (Cognizant)
Wed
3 / 20 / 2019
6: 16
PM
https: // spark.apache.org / docs / latest / sql - getting - started.html
https: // towardsdatascience.com / a - brief - introduction - to - pyspark - ff4284701873
https: // towardsdatascience.com / 10 - python - pandas - tricks - that - make - your - work - more - efficient - 2e8
e483808ba
https: // towardsdatascience.com / six - recommendations -
for -aspiring - data - scientists - 93d12aeb9b9
Ray, Riju (Cognizant)
Wed 3 / 20 / 2019 3:21
PM
""" spark.py ~~~~~~~~ Module containing helper function for use with Apache Spark """
import __main__
from os import environ, listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from dependencies import logging


def
