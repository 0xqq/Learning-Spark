# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# @Author: appleyuchi
# @Date:   2018-07-27 13:02:42
# @Last Modified by:   appleyuchi
# @Last Modified time: 2018-07-27 15:08:45
import re
from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
import subprocess
conf=SparkConf().setAppName("example")
sc=SparkContext(conf=conf)
ret = subprocess.call(["rm", "-r","contactCount"], shell=False)#删除已经生成的contactCount文件夹
"""Contains the Chapter 6 Example illustrating accumulators, broadcast
variables, numeric operations, and pipe."""
import bisect
import re
import sys
import urllib3
import json
import math
import os

from pyspark import SparkContext
from pyspark import SparkFiles


inputFile='./input.txt'

file = sc.textFile(inputFile)
global validSignCount
global invalidSignCount


validSignCount = sc.accumulator(0)
invalidSignCount = sc.accumulator(0)



def validateSign(sign):
    global validSignCount, invalidSignCount
    if re.match(r"\A\d?[a-zA-Z]{1,2}\d{1,4}[a-zA-Z]{1,3}\Z", sign):

        validSignCount += 1
        return True
    else:
        invalidSignCount += 1
        return False


callSigns = sc.parallelize(["2re7777sfs","KK6JLK"])#这个不是正规的电台号,是我根据上面的正则匹配表达式设计出来的
#可以使用网站http://tool.oschina.net/regex/来设计表达式
validSigns = callSigns.filter(validateSign)
contactCounts = validSigns.map(lambda sign: (sign, 1)).reduceByKey((lambda x, y: x + y))
# Force evaluation so the counters are populated
contactCounts.count()
if invalidSignCount.value < 0.1 * validSignCount.value:
    contactCounts.saveAsTextFile("result")
else:
    print ("Too many errors %d in %d" % (invalidSignCount.value, validSignCount.value))

# Helper functions for looking up the call signs
#整个代码的意思就是看下电台号是不是符合规定,属于业务逻辑