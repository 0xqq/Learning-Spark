# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# @Author: appleyuchi
# @Date:   2018-08-10 13:22:29
# @Last Modified by:   appleyuchi
# @Last Modified time: 2018-08-10 14:22:25

#注意!!!
# 以下代码运行前,必须启动hdfs以及metastore服务,具体步骤如下
# 一,./start-dfs.sh
# 二,hive --service metastore &



#-------------------------example9-5-----------------------
from pyspark.sql import HiveContext,Row
from pyspark.sql import SQLContext,Row
from pyspark import SparkContext

from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
conf=SparkConf().setAppName("spark_json1")

#-------------------------example9-8-----------------------
sc=SparkContext(conf=conf)
hiveCtx=HiveContext(sc)

#-------------------------example9-11-----------------------
inputFile="student.json"
#因为这本书提供的下载json的api已经过期了,所以怎么办呢?
#我们换个文件,其中一个办法就是从mongodb中导出json文件来让这里使用
#因为mongodb导出的是数据库文件,同时也是json文件,所以和这里一拍即合.



# inputs=hiveCtx.jsonFile(inputFile)#书上的这句代码用下面这句话代替,书上是老版本的spark和hive,这里使用spark2.3.1和hive3.0.0运行
inputs = hiveCtx.read.json(inputFile)#根据书上的说法,SchemaRDD相当于数据库中的表格

print "input的类型:",type(inputs)#通过这里的输出可以看到,早期的SchemaRDD已经改名叫<class 'pyspark.sql.dataframe.DataFrame'>了
# 与书的P146页的9.2.3小结提到的名字变动一事完全一致


# inputs=SQLContext.read().json(inputFile)
# inputs=hiveCtx.read().format("Json").load(inputFile)
inputs.registerTempTable("tweets")#这里之所以要注册为临时表的原因是,SQL语言只能对表格进行查询
topTweets=hiveCtx.sql("""SELECT * from tweets order by age limit 10""")


print type(topTweets)#<class 'pyspark.sql.dataframe.DataFrame'>
topTweets.show

print"--------------------------example9-14---------------------------------------------------"

# topTweets=topTweets.map(lambda row:row.text)#书上的这句话用下面这句话代替,书上是老版本的spark和hive,这里使用spark2.3.1和hive3.0.0运行
topTweets.select("name").show()#网上的博客很坑,都是topTweets.select("name").show,没有中括号,所以注意这里一定不要漏下中括号,否则会没有输出结果的.