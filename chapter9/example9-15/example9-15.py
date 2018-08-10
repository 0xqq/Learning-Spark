# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


#注意!!!
# 以下代码运行前,必须启动hdfs以及metastore服务,具体步骤如下
# 一,./start-dfs.sh
# 二,hive --service metastore &
# 三,必须hive配置好元数据库,命令如下:
# hadoop fs -mkdir -p /user/hive/warehouse


# (python2.7) appleyuchi@ubuntu:~$ hdfs dfs -ls /user/hive/warehouse
# Found 1 items
# drwxr-xr-x   - appleyuchi supergroup          0 2018-08-10 15:58 /user/hive/warehouse/hive_yuchi.db



# 然后建表:
# hive> 
#     > create table mytable
#     > (id int, name string,
#     > age int, tel string)
#     > ROW FORMAT DELIMITED
#     > FIELDS TERMINATED BY '\t'
#     > STORED AS TEXTFILE;
# OK




#再导入数据
# hive> load data local inpath 'Desktop/wyp.txt' into table mytable;

# 其中:
# wyp.txt的路径为:
# /home/appleyuchi/Desktop/wyp.txt









#---------------------------------example9-15-----------------------------------------
from pyspark.sql import HiveContext
from pyspark import SparkConf,SparkContext
conf=SparkConf().setAppName("spark_json1")
sc=SparkContext(conf=conf)
hiveCtx=HiveContext(sc)


rows=hiveCtx.sql("select * from hive_yuchi.mytable")#这里from后面使用的是"数据库名.表名"的访问格式
# keys=rows.map(lambda row:row[0])
rows.select("name").show()








