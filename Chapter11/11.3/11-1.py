# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf8')
# @Author: appleyuchi
# @Date:   2018-08-17 15:32:12
# @Last Modified by:   appleyuchi
# @Last Modified time: 2018-08-20 17:15:33

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint

from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.feature import HashingTF


def g(x):
    print type(x)
    print x
if __name__ == "__main__":
    import sys
    reload(sys)
    sys.setdefaultencoding('utf8')
    sc = SparkContext(appName="PythonBookExample")

    # Load 2 types of emails from text files: spam and ham (non-spam).
    # Each line has text from one email.
    spam_file=sys.path[0]+"/spam.txt"#获得当前路径
    ham_file=sys.path[0]+"/ham.txt"#获得当前路径
    spam_path="file://"+spam_file
    ham_path="file://"+ham_file
    print"ham_path=",ham_path
    print"spam_path=",spam_path
    spam = sc.textFile(spam_path)
    ham=sc.textFile(ham_path)

    print "-----------------------------------------------------------------------------------------------------------"
    print type(spam)
    # Create a HashingTF instance to map email text to vectors of 100 features.

    tf = HashingTF(numFeatures = 100)

    # Each email is split into words, and each word is mapped to one feature.

    spamFeatures = spam.map(lambda email: tf.transform(email.split(" ")))#这里的email表示一条数据
    hamFeatures   =  ham.map(lambda email: tf.transform(email.split(" ")))

    # Create LabeledPoint datasets for positive (spam) and negative (ham) examples.
    positiveExamples = spamFeatures.map(lambda features: LabeledPoint(1, features))
    negativeExamples = hamFeatures.map(lambda features: LabeledPoint(0, features))
    training_data = positiveExamples.union(negativeExamples)
    training_data.cache() # Cache data since Logistic Regression is an iterative algorithm.

    # # 这个地方值得留意,可以看到,training_data在train之前被cache了一下.

    # # Run Logistic Regression using the SGD optimizer.
    # # regParam is model regularization, which can make models more robust.
    model = LogisticRegressionWithSGD.train(training_data)

    # # Test on a positive example (spam) and a negative one (ham).
    # # First apply the same HashingTF feature transformation used on the training data.
    posTestExample = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    negTestExample = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))
    # print(posTestExample)

    # # Now use the learned model to predict spam/ham for new emails.
    print "Prediction for  positive test example: %g" % model.predict(posTestExample)
    print "Prediction for negative test example: %g" % model.predict(negTestExample)

    # sc.stop()
