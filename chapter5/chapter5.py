# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# @Author: appleyuchi
# @Date:   2018-07-19 14:59:02
# @Last Modified by:   appleyuchi
# @Last Modified time: 2018-07-20 14:59:51
import subprocess
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
conf = SparkConf(). setMaster( "local"). setAppName( "My App")
sc = SparkContext( conf = conf)
lines=sc.textFile("README.md")
# print"type of lines:",type(lines)
# print type(lines.collect())#这个collect相当于是RDD转化为python中的ｌｉｓｔ


# def g(x):
#     print x
#     print"\n"
# print"-----------------Example 5-1--------------------------------------"
# input=sc.textFile("./README.md")


# print"-----------------Example 5-5--------------------------------------"
# import subprocess
# ret = subprocess.call(["rm", "-r","result"], shell=False)#必须对原来的result文件夹进行删除，否则后面运行代码时会报错．

# outputFile='./result'#注意，这里是一个文件夹，放置各种数据
# data = [1, 2, 3, 4, 5]
# result = sc.parallelize(data, numSlices=10)
# result.saveAsTextFile(outputFile)#注意，运行这句代码的时候，需要删除当前文件夹下面的result文件夹，否则会报错


# print"-----------------Example 5-６--------------------------------------"
# import json
# try:
#     sc.stop() #停止以前的SparkContext，要不然下面创建工作会失败
# except:
#     pass

# import json
# sc=SparkContext('local','JSONAPP')
# inputFile="./testweet.json"
# input=sc.textFile(inputFile)
# result=input.map(lambda x : json.loads(x))
# print type(result)
def g(x):
    print x
# result.foreach(g)

# print"-----------------Example 5-9-------这个代码运行前要注意删除当前路径下面的------------------------------"
# import json
# inputFile='./testweet.json'
# ret = subprocess.call(["rm", "-r","panda_result"], shell=False)
# outputFile='./panda_result'
# sc = SparkContext( conf = conf)
# input = sc.textFile(inputFile)
# data = input.map(lambda x: json.loads(x))
# print data.collect()
# (data.filter(lambda x:x["lovesPandas"]).map(lambda x: json.dumps(x)).saveAsTextFile(outputFile))
# #这句话其实没有筛选作用，只有保存为json格式的文件的作用，但是没有保存为.json文件的功能


# print"-----------------Example 5-12----------------------------------------------------"
# import csv
# import StringIO
# inputFile='./names.csv'
# # ret = subprocess.call(["rm", "-r","name_result"], shell=False)
# outputFile='name_result'
# def loadRecord(line):
#     input=StringIO.StringIO(line)
#     reader=csv.DictReader(input,fieldnames=["first_name","last_name"])
#     print"type of reader=",type(reader)
#     return reader.next()
# input=sc.textFile(inputFile).map(loadRecord)#这里首次展示了def代替lambda函数的方法，也就是输入loadRecord，返回reader.next()


# print"-----------------Example 5-15----------------------------------------------------"
# def loadRecords(fileNameContents):
#     input=StringIO.StringIO(fileNameContents[1])
#     reader=csv.DictReader(input,fieldnames=["first_name","last_name"])
#     return reader
# fullFileData=sc.wholeTextFiles(inputFile).flatMap(loadRecords)
# print type(fullFileData)#<class 'pyspark.rdd.PipelinedRDD'>
# fullFileData.foreach(g)



# print"-----------------Example 5-18----------------------------------------------------"
# def writeRecords(records):
#     output=StringIO.StringIO()
#     write=csv.DictWriter(output,fieldnames=["first_name","last_name"])
#     for record in records:
#         write.writerow(record)
#     return[output.getvalue()]
# def loadRecord(line):
#     """Parse a CSV line"""
#     input = StringIO.StringIO(line)
#     reader = csv.DictReader(input, fieldnames=["first_name","last_name"])
#     return reader.next()
# def loadRecords(fileNameContents):
#     """Load all the records in a given file"""
#     input = StringIO.StringIO(fileNameContents[1])
#     reader = csv.DictReader(input, fieldnames=["first_name","last_name"])
#     return reader

# outputFile='./Example5-18'

# data = fullFileData
# print"---------------data的类型与内容------------------------"
# print data
# print type(data)
# print data.foreach(g)

# print"---------------------------------------"
# pandaLovers = data.filter(lambda x: x['last_name'] == "Beans")
# ret = subprocess.call(["rm", "-r","Example5-18"], shell=False)

# pandaLovers.mapPartitions(writeRecords).saveAsTextFile(outputFile)
# print "type of pandaLovers=",type(pandaLovers)
# print pandaLovers.foreach(g)
print"-----------------Example 5-20书上代码有误,误用了scala----------------------------------------------------"
print"-----------------下面先是序列化,写入SequenceFile-------------------"
rdd = sc.parallelize(["2,Fitness", "3,Footwear", "4,Apparel"])
ret = subprocess.call(["rm", "-r","testSeq"], shell=False)
rdd.map(lambda x: tuple(x.split(",", 1))).saveAsSequenceFile("testSeq")
ret = subprocess.call(["rm", "-r","testSeqNone"], shell=False)
rdd.map(lambda x: (None, x)).saveAsSequenceFile("testSeqNone")#这的意思是保留整个字符串

print"-----------------再是反序列化，读取SequenceFile-------------------"
Text = "org.apache.hadoop.io.Text"
print (sc.sequenceFile("./testSeq/part-00000", Text, Text).values().first())
print"------------------------------------"
result=sc.sequenceFile("./testSeqNone/part-00000", Text, Text).values()
print type(result)
print result.foreach(g)
print (sc.sequenceFile("./testSeqNone/part-00000", Text, Text).values().first())



print"-----------------Example 5-30-----注意这个例子需要安装好hive-------------------------------------------"
from pyspark.sql import HiveContext
hiveCtx=HiveContext(sc)
rows=hiveCtx.sql("SELECT name,age from users")
firstRow=rows.first()
print firstRow.name
