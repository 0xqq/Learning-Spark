# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# @Author: appleyuchi
# @Date:   2018-07-27 17:05:37
# @Last Modified by:   appleyuchi
# @Last Modified time: 2018-07-27 19:42:20
#本代码无法正常运行,因为获取

import os
import re
import urllib3
from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
from pyspark import SparkFiles
import json
from pyspark import SparkContext
conf=SparkConf().setAppName("example")
sc=SparkContext(conf=conf)


distScriptName = "./files/finddistance.R"

validSignCount = sc.accumulator(0)
invalidSignCount = sc.accumulator(0)


def processCallSigns(signs):
    """Lookup call signs using a connection pool"""
    # Create a connection pool
    http = urllib3.PoolManager()
    # the URL associated with each call sign record
    urls = map(lambda x: "http://73s.com/qsos/%s.json" % x, signs)
    #注意,该书提供的所有callsign中,除了N7ICE以外,其他都过期了.验证是否过期的方法是:
    #curl -X GET http://73s.com/qsos/N7ICE.json
    #可以看到,除了callsign N7ICE以外,其他的callsign发送请求时,返回的都是null

    # create the requests (non-blocking)
    requests = map(lambda x: (x, http.request('GET', x)), urls)
    # fetch the results
    result = map(lambda x: (x[0], json.loads(x[1].data)), requests)
    # remove any empty results and return
    return filter(lambda x: x[1] is not None, result)
def validateSign(sign):
    global validSignCount, invalidSignCount
    if re.match(r"\A\d?[a-zA-Z]{1,2}\d{1,4}[a-zA-Z]{1,3}\Z", sign):
        validSignCount += 1
        return True
    else:
        invalidSignCount += 1
        return False
def fetchCallSigns(input):
    """Fetch call signs"""
    return input.mapPartitions(lambda callSigns: processCallSigns(callSigns))


callSigns = sc.parallelize(["3CZ7777sfs","4CZ7777sfs"])#这个不是正规的电台号,是我根据上面的正则匹配表达式设计出来的
validSigns = callSigns.filter(validateSign)
# contactCounts = validSigns.map(lambda sign: (sign, 1)).reduceByKey((lambda x, y: x + y))

contactsContactList = fetchCallSigns(validSigns)
pipeInputs = contactsContactList.values().flatMap(
    lambda calls: map(formatCall, filter(hasDistInfo, calls)))

distances = pipeInputs.pipe(SparkFiles.get(distScriptName))
print "type of distances=",type(distances)
print distances.collect()
# Convert our RDD of strings to numeric data so we can compute stats and
# remove the outliers.
distanceNumerics = distances.map(lambda x: float(x))


def g(x):
    print x


print"type of distanceNumerics=",type(distanceNumerics)
stats = distanceNumerics.stats()
stddev = stats.stdev()
mean = stats.mean()
reasonableDistances = distanceNumerics.filter(
    lambda x: math.fabs(x - mean) < 3 * stddev)
print reasonableDistances.collect()
