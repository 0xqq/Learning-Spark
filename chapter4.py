# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# @Author: appleyuchi
# @Date:   2018-07-14 16:29:23
# @Last Modified by:   appleyuchi
# @Last Modified time: 2018-07-14 16:39:26
from pyspark import SparkConf, SparkContext
conf = SparkConf(). setMaster( "local"). setAppName( "My App")
sc = SparkContext( conf = conf)
# lines=sc.textFile("README.md")
# pairs = lines. map( lambda x: (x. split( " ")[0 ], x ))
# print pairs

# Caused by: java.net.UnknownHostException: ubuntu: ubuntu: Name or service not known