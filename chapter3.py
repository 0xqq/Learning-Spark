# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# @Author: appleyuchi
# @Date:   2018-07-13 20:58:10
# @Last Modified by:   appleyuchi
# @Last Modified time: 2018-07-14 15:39:26
from pyspark import SparkConf, SparkContext
conf = SparkConf(). setMaster( "local"). setAppName( "My App")
sc = SparkContext( conf = conf)
lines=sc.textFile("README.md")
pythonLines=lines.filter(lambda line:"Python" in line)
pythonLines.first()
# u'* The Python examples require urllib3'                                        
pythonLines.persist
# <bound method PipelinedRDD.persist of PythonRDD[4] at RDD at PythonRDD.scala:49>
pythonLines.count()
# 2
pythonLines.first()
# The Python examples require urllib3'
lines=sc.parallelize(["pandas","i like pandas"])
lines=sc.textFile("README.md")
inputRDD=sc.textFile("log.txt")
errorRDD=inputRDD.filter(lambda x:"error" in x)
errorRDD=inputRDD.filter(lambda x:"error" in x)
warningsRDD=inputRDD.filter(lambda x:"warning" in x)
badLinesRDD=errorRDD.union(warningsRDD)#这里的意思就是加了两个filter的结果,但是注意，这个是或的关系，不是且的关系，也就是说，只要有一个filter的要求被满足，那么文章中的这个句子就会被输出
print"Input had "+ str(badLinesRDD.count())+"concerning lines"
# Input had 2concerning lines

print"Input had "+ str(badLinesRDD.count())+"concerning lines"
for line in badLinesRDD.take(10):
    print line

def containsError(s):
    return "error" in s
word = inputRDD.filter(containsError)
#Example19略过，因为书上说这个是错的，不要进行实验
class WordFunctions(object):
    def getMatchesNoReference( self, rdd):
 # Safe: extract only the field we need into a local variable
        query = self. query
        return rdd. filter( lambda x: query in x)

#接下来是Example 3-26

#下面的代码都属于RDD的Transformations（其实就是数据的清晰或者删减）
nums = sc. parallelize([1, 2, 3, 4 ])
squared = nums.map( lambda x: x * x). collect()
for num in squared:
    print " %i " % ( num)

print"nums=",nums

lines = sc. parallelize(["hello world", "hi"])
words = lines. flatMap( lambda line:line.split( " "))
words. first() # returns "hello"


#下面的代码都属于RDD的Action（其实就是涉及数据的计算）


sum = inputRDD. reduce(lambda x, y:x+y)#这个意思是，入口参数是x和y，返回x+y


# #下面的acc和value以及acc1和acc2都是匿名函数的入口参数名字。
sumCount = nums.aggregate(\
(1,0,0), #initial value 
(lambda acc, value: (acc[0]*value,acc[1] + value, acc[2] + 1)), #combine value with acc 
(lambda acc1, acc2: (acc1[0]*acc2[0],acc1[1]+acc2[1],acc1[2]+acc2[2])))#combine accumulators )#acc1[0]+acc2[0]接收各个分区的value，acc1[1]+acc2[1]接收各个分区的元素统计数量

# 这里为什么是(1,0,0)呢？
# 因为这里我们想要的数据是：累乘的和，累加的和，以及元素数量统计 共3个参数，所以我们设定初始值为(1,0,0)
# 注意，这里value会“并行遍历”nums中的每一个元素，这里的初始值赋值情况为：
# acc[0]=1
# acc[1]=0
# acc[2]=0
# 可以看到这里的1，0，0和初始值(1,0,0)是完全一致的。
#注意这里是“分区+并行计算”的思想
# 因为nums中的元素是4个，所以可以理解为四个子分区，相当于map
#acc1[0]*acc2[0]可以理解为每个分区的结果相乘，
#acc1[1]+acc2[1]的结果相加
#acc1[2]+acc2[2]可以理解为每个分区的结果相加
# 总体而言：第二个lambda函数在并行处理第一个lambda函数的返回结果，相当于reduce（也就是各路结果汇总）

#另外注意，spark在单机运行的效果与多进程是一样（注意这里不是多线程，python的多线程是串行的，不具备并行效果）
#所以spark的优势其实是在多台机子上



print sumCount

print sumCount[0]/float(sumCount[1])
