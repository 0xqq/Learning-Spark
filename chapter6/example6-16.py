# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# @Author: appleyuchi
# @Date:   2018-07-27 16:54:19
# @Last Modified by:   appleyuchi
# @Last Modified time: 2018-07-27 19:41:42
import os
import re
import urllib3
from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
from pyspark import SparkFiles
import json
# Compute the distance of each call using an external R program
distScript ="./files/finddistance.R"
distScriptName = "finddistance.R"

from pyspark import SparkContext
conf=SparkConf().setAppName("example")
sc=SparkContext(conf=conf)
sc.addFile(distScript)
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
    print"urls=",urls 


    # create the requests (non-blocking)
    requests = map(lambda x: (x, http.request('GET', x)), urls)
    # fetch the results
    result = map(lambda x: (x[0], json.loads(x[1].data)), requests)
    # remove any empty results and return
    return filter(lambda x: x[1] is not None, result)


def hasDistInfo(call):
    """Verify that a call has the fields required to compute the distance"""
    requiredFields = ["mylat", "mylong", "contactlat", "contactlong"]
    return all(map(lambda f: call[f], requiredFields))

def fetchCallSigns(input):
    """Fetch call signs"""
    return input.mapPartitions(lambda callSigns: processCallSigns(callSigns))


def formatCall(call):
    """Format a call so that it can be parsed by our R program"""
    return "{0},{1},{2},{3}".format(
        call["mylat"], call["mylong"],
        call["contactlat"], call["contactlong"])

def validateSign(sign):
    global validSignCount, invalidSignCount
    if re.match(r"\A\d?[a-zA-Z]{1,2}\d{1,4}[a-zA-Z]{1,3}\Z", sign):
        validSignCount += 1
        return True
    else:
        invalidSignCount += 1
        return False

lists=[
"W8PAL",
"KK6JKQ",
"W6BB",
"VE3UOW",
"VE2CUA",
"VE2UN",
"OH2TI",
"GB1MIR",
"K2AMH",
"UA1LO",
"N7ICE"]

callSigns = sc.parallelize(lists)#这个不是正规的电台号,是我根据上面的正则匹配表达式设计出来的
validSigns = callSigns.filter(validateSign)
# print"validSigns:",validSigns.collect()
print"------------------------------------------------------"


contactsContactList = fetchCallSigns(validSigns)
print"contactsContectList:",contactsContactList.collect()
print"------------------------------------------------------"
pipeInputs = contactsContactList.values().flatMap(lambda calls: map(formatCall, filter(hasDistInfo, calls)))
distances = pipeInputs.pipe(SparkFiles.get(distScriptName))
print distances.collect()