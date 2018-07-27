# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# @Author: appleyuchi
# @Date:   2018-07-27 11:25:48
# @Last Modified by:   appleyuchi
# @Last Modified time: 2018-07-27 11:29:18

#注意运行前必须启动hive --service metastore
from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
conf=SparkConf().setAppName("spark_json1")
sc=SparkContext(conf=conf)
hiveCtx=HiveContext(sc)
tweets=hiveCtx.read.json("./testweet.json")
tweets.registerTempTable("tweets")
results=hiveCtx.sql("select * from tweets")
print results.collect()
#注意这个结果只是临时表,所以在hive数据库中是不会有写入的.