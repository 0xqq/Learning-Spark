# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# @Author: appleyuchi
# @Date:   2018-07-27 12:50:47
# @Last Modified by:   appleyuchi
# @Last Modified time: 2018-07-27 13:00:57


from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
import subprocess
conf=SparkConf().setAppName("spark_json1")
sc=SparkContext(conf=conf)
ret = subprocess.call(["rm", "-r","result"], shell=False)#删除已经生成的result文件夹
inputFile="./test.txt"
file=sc.textFile(inputFile)
blankLines=sc.accumulator(0)

def extractCallSigns(line):
    if(line==""):
        global blankLines
        blankLines+=1
    return line.split(" ")
callSigns=file.flatMap(extractCallSigns)
callSigns.saveAsTextFile("result")
print"Blank lines: %d"% blankLines.value#从最终结果可以看到,统计的是整个文本的所有空行