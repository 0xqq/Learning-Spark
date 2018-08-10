# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# @Author: appleyuchi
# @Date:   2018-08-10 16:35:48
# @Last Modified by:   appleyuchi
# @Last Modified time: 2018-08-10 16:55:53



# encoding:utf-8
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import Row

# Basic Data Configuration
APP_NAME = "DataClean1_getHDFSparquetFile"
parquetFile = "hdfs://127.0.0.1:9000/user/appleyuchi/people2.parquet"  # Which parquetFile to read
sparkURL = "spark://127.0.0.1:7077"
HADOOP_USER_NAME = "hadoop"

# Test Configuration
jsonFile = "hdfs://127.0.0.1:9000/user/appleyuchi/people.json"


conf = SparkConf().setAppName("json2parquet")
sc = SparkContext(conf=conf)




hive_ctx = HiveContext(sc)
input1 = hive_ctx.read.json(jsonFile)
input1.registerTempTable("people")
top_tweets = hive_ctx.sql("SELECT name,age FROM people")

assert isinstance(top_tweets, DataFrame)
top_tweets.select("name","age").write.format("parquet").save("hdfs://127.0.0.1:9000/user/appleyuchi/people2.parquet")
