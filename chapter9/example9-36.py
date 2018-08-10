# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# @Author: appleyuchi
# @Date:   2018-08-10 18:37:47
# @Last Modified by:   appleyuchi
# @Last Modified time: 2018-08-10 18:51:34
from pyspark.sql import HiveContext,Row
from pyspark.sql import SQLContext,Row
from pyspark.sql import HiveContext

from pyspark import SparkConf,SparkContext
from pyspark import SparkContext
from pyspark import SparkFiles

conf=SparkConf().setAppName("example")
sc=SparkContext(conf=conf)
hiveCtx=HiveContext(sc)
sc=SQLContext(sc)
# hiveCtx.registerFunction("strLenPython",lambda x:len(x),IntegerType())书上这句代码报错,看了下文档上的源码中写的是StringType(),所以我改为不写最后一个参数
# 文档链接为:
# http://spark.apache.org/docs/preview/api/python/_modules/pyspark/sql/context.html
hiveCtx.registerFunction("strLenPython",lambda x:len(x))
lengthSchemaRDD=hiveCtx.sql("select strLenPython('name') from hive_yuchi.mytable limit 10")
print"--------------------------------------------"
lengthSchemaRDD.select("*").show()#感觉这个效果不是太好,返回的是建立hive表格时,name的自适应的最大占用字符长度.





