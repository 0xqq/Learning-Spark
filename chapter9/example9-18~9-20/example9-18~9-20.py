# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# @Author: appleyuchi
# @Date:   2018-08-10 16:56:49
# @Last Modified by:   appleyuchi
# @Last Modified time: 2018-08-10 18:28:09

from pyspark.sql import HiveContext,Row
from pyspark.sql import SQLContext,Row
from pyspark.sql import HiveContext

from pyspark import SparkConf,SparkContext
from pyspark import SparkContext
from pyspark import SparkFiles


conf=SparkConf().setAppName("example")
sc=SparkContext(conf=conf)
hiveCtx=HiveContext(sc)
sc=SQLContext(sc)#这种开头网上真是不好找啊☆☆☆☆☆☆☆☆☆☆

sqlContext = SQLContext(sc)

parquetfile_path="hdfs://127.0.0.1:9000/user/appleyuchi/people2.parquet"

#---------------------example9-18-----------------------------------------
# rows=hiveCtx.parquetFile(parquetfile_path)#这句代码在新版本中已经失效了,使用下列代码代替:
rows =sc.read.load(parquetfile_path)
print type(rows)
# names=rows.map(lambda row:row.name)
# print"Everyone"
# print names.collect()


#以上几句代码是老版本的,已经在新版本中失效了,我们使用新版本的代码如下:
rows.select("name","age").show()

print"------------------example9-19----------------------------------------------------"
tbl=rows.registerTempTable("people")
pandaFriends=hiveCtx.sql("  select * from hive_yuchi.mytable where name='wyp'   ")#注意这里hive_yuchi.mytable 是"数据库名.表格名"的形式
print type(pandaFriends)
print "Panda friends"
# print pandaFriends.map(lambda row:row.name).collect()#这个代码已经过时,使用下面的代码代替:
pandaFriends.select("name","age").show()#这个其实相当于hiveSQL中 select name,age from table_name的命令效果
