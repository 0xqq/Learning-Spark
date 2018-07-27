# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# @Author: appleyuchi
# @Date:   2018-07-21 16:56:40
# @Last Modified by:   appleyuchi
# @Last Modified time: 2018-07-27 11:18:18

#代码运行前，必须运行hive --service metastore
#
from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
conf=SparkConf().setAppName("spark_sql_test")
sc=SparkContext(conf=conf)#这句话再pyspark的交互模式中必须去掉
hiveCtx=HiveContext(sc)
rows=hiveCtx.sql("show databases")
print rows.collect()

hiveCtx.sql("use hive_yuchi")

result=hiveCtx.sql("select * from users")
print result.collect()
