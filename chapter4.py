# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# @Author: appleyuchi
# @Date:   2018-07-14 16:29:23
# @Last Modified by:   appleyuchi
# @Last Modified time: 2018-07-19 14:24:21
from pyspark import SparkConf, SparkContext
conf = SparkConf(). setMaster( "local"). setAppName( "My App")
sc = SparkContext( conf = conf)
lines=sc.textFile("README.md")


def g(x):
    print x
    print"\n"

# print"-----------------Example 4-1--------------------------------------"

# pairs = lines. map( lambda x: (x.split(" ")[0],x))#返回分词后的第一个单词,以及整个句子x
# print"type of pairs",type(pairs)
# print pairs.collect()#用来输出RDD中的内容

# print"----------------Example 4-4----------------------------------------------"
# result=pairs.filter(lambda keyValue:len(keyValue[1])<20)#他这里的key-value=(一句话开头的那个单词,整句话)
# #这里面的pairs是整个文章，pairs->第一个lambda存在降级，也就是每一行作为keyValue输入，因为是filter的功能，所以输入的每一行中，如果该行的长度＜40，那么就保留、返回该行。
# print result.collect()

# print"----------------Example 4-7-----------------------------------------------"
# result=pairs.mapValues(lambda x:(x,1))
# print result.collect()
# result.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
# print result.collect()
# print"----------------Example 4-9-----------------------------------------------"
# rdd=sc.textFile("./README1.md")
# words=rdd.flatMap(lambda x:x.split(" "))
# result=words.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)#
# print result.collect()


# print"----------------Example 4-12----------------------------------------------"
# nums = sc.parallelize([("coffee", 1), ("pandas", 2), ("coffee", 3), ("very", 4)])
# def perKeyAvg(nums):
#     """Compute the avg"""
#     sumCount = nums.combineByKey((lambda x: (x, 1)),#这个意思是，把相同key的归为一组
#                                  (lambda x, y: (x[0] + y, x[1] + 1)),
#                                  (lambda x, y: (x[0] + y[0], x[1] + y[1])))
#     return sumCount.collectAsMap()

# avg = perKeyAvg(nums)
# print avg
print"----------------Example 4-15--------------------------------------------"
data=[("a",3),("b",4),("a",1)]
result1=sc.parallelize(data).reduceByKey(lambda x,y:x+y)
print result1.collect()
result2=sc.parallelize(data).reduceByKey(lambda x,y:x+y,10)
print result2.collect()


print"----------------Example 4-19--------------------------------------------"
rdd=sc.textFile("./README.md")

result=rdd.sortByKey(ascending=True,numPartitions=None,keyfunc=lambda x:str(x))#这个是按照首字母在排序的．
print result.collect()


print"----------------Example 4-27--------------------------------------------"

import urlparse
def hash_domain(url):
    return hash(urlparse.urlparse(url).netloc)
result=rdd.partitionBy(20,hash_domain)