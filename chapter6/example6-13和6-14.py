# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# @Author: appleyuchi
# @Date:   2018-07-27 16:26:34
# @Last Modified by:   appleyuchi
# @Last Modified time: 2018-07-27 16:50:37

import sys

from pyspark import SparkContext
#--------------------------example6-13----------------------------------------------------------------------


def combineCtrs(c1, c2):
    print "c1=",c1
    print "c2=",c2
    return (c1[0] + c2[0], c1[1] + c2[1])


def basicAvg(nums):
    """Compute the avg"""
    sumCount = nums.mapPartitions(partitionCtr).reduce(combineCtrs)
    return sumCount[0] / float(sumCount[1])
#------------------------------------------------------------------------------------------------

def partitionCtr(nums):
    """Compute sumCounter for partition"""
    sumCount = [0, 0]
    for num in nums:
        sumCount[0] += num
        sumCount[1] += 1
    return [sumCount]

def fastAvg(nums):
    sumCount=nums.mapPartitions(partitionCtr).reduce(combineCtrs)
    return sumCount[0]/float(sumCount[1])

#------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    # ############对example6-13进行测试################################
    # cluster = "local"
    # if len(sys.argv) == 2:
    #     cluster = sys.argv[1]
    # sc = SparkContext(cluster, "Sum")
    # nums = sc.parallelize([1, 2, 3, 4])
    # avg = basicAvg(nums)
    # print avg

    ############3这个代码只是对partitionCtr进行测试

    # ############对example6-14进行测试################################
    from pyspark import SparkConf,SparkContext
    from pyspark.sql import HiveContext
    import subprocess
    import re
    import bisect

    conf=SparkConf().setAppName("example")
    sc=SparkContext(conf=conf)
    nums=[1,2,3,4]
    nums=sc.parallelize(nums)
    print"type of nums=",type(nums)
    result=fastAvg(nums)
    print "result=",result
