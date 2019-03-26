import os
import sys


try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    print ("Successfully imported Spark Modules")
except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)

sc = SparkContext('local')
words = sc.parallelize(["scala","java","hadoop","spark","akka"])
print(words.count())
