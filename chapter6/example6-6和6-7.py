# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# @Author: appleyuchi
# @Date:   2018-07-27 13:02:42
# @Last Modified by:   appleyuchi
# @Last Modified time: 2018-07-27 16:19:48


from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
import subprocess
import re
import bisect

conf=SparkConf().setAppName("example")
sc=SparkContext(conf=conf)
def loadCallSignTable():
    f = open("./files/callsign_tbl_sorted", "r")
    return f.readlines()
ret = subprocess.call(["rm", "-r","result"], shell=False)
# signPrefixes = loadCallSignTable()#example6-6就是这句话,其余一致
signPrefixes = sc.broadcast(loadCallSignTable())#example6-7就是这句话,其余一致
print type(signPrefixes)
print"-"*50
validSignCount = sc.accumulator(0)
invalidSignCount = sc.accumulator(0)

def validateSign(sign):
    if re.match(r"\A\d?[a-zA-Z]{1,2}\d{1,4}[a-zA-Z]{1,3}\Z", sign):
        global validSignCount
        validSignCount += 1
        return True
    else:
        global invalidSignCount
        invalidSignCount += 1
        return False
def lookupCountry(sign, prefixes):

    print"prefixes=",prefixes
    print"sign=",sign

    pos = bisect.bisect_left(prefixes, sign)
    print"pos=",pos
    return prefixes[pos-1].split(",")[1]
def processSignCount(sign_count, signPrefixes):
    print"☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆"
    print"sign_count=",sign_count
    print"sign_count[0]=",sign_count[0]
    # print"signPrefixes.value=",signPrefixes.value
    print"☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆☆"
    country = lookupCountry(sign_count[0], signPrefixes.value)
    count = sign_count[1]
    print"country,count=",(country, count)
    return (country, count)

##############################################################################################################3

callSigns = sc.parallelize(["3CZ7777sfs","4CZ7777sfs"])#这个不是正规的电台号,是我根据上面的正则匹配表达式设计出来的
#可以使用网站http://tool.oschina.net/regex/来设计表达式
validSigns = callSigns.filter(validateSign)
print"-------------------------"
print"validSigns=",validSigns.collect()
print"-------------------------"


contactCounts = validSigns.map(lambda sign: (sign, 1)).reduceByKey((lambda x, y: x + y))
print"-------------------------"
print"contactCounts=",contactCounts.collect()
print"########################"
countryContactCounts = (contactCounts
                        .map(lambda signCount: processSignCount(signCount, signPrefixes))
                        .reduceByKey((lambda x, y: x + y)))

countryContactCounts.saveAsTextFile("result")


#这个代码的目的是根据电报的callsign来判断到底给发给哪个国家
#但是,这个代码到底什么问题?首先,如果我给出错误的callsign,他就会给出错误的结果,也就是会根据一个不存在的callsign给出一个胡乱匹配的国家
#也就是说,这个代码只能查询 files/callsign_tbl_sorted中已经存在的前缀,例如3CZ,如果想查询该文件中不存在的callsign,就会给出错误的结果
#另外,作者提供的代码中,pos忘记-1了,这里已经修正