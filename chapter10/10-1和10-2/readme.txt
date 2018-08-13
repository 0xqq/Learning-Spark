準備工作，文件結構如下：

(python2.7) appleyuchi@ubuntu:~/Desktop/test3/maven+scala+streaming$ tree
.
├── pom.xml
├── READ.me
├── src
│   └── main
│       └── scala
│           └── wordcount
│               └── WordCount.scala

下面是詳細的步驟

1.
注意运行这个例子前，还需要启动hdfs，否则会connection refused
具體命令是：
./start-dfs.sh
然後jps命令看下，是否namenode和datanode都運行起來了。

3.必须是在pom.xml所在目录的路径下，运行以下命令：
mvn clean scala:compile compile package

4.
开两个终端：

终端1输入：
(python2.7) appleyuchi@ubuntu:~/Learning-Spark/chapter10/example1-9/target$ nc -lk 9999
you go the error way

终端2输入：
/home/appleyuchi/bigdata/spark-2.3.1-bin-hadoop2.7/bin/spark-submit \
--class "WordCount"  \
~/Learning-Spark/chapter10/example1-9/target/java-0.0.2.jar


##################终端2上的运行结果################
Time: 1533900070000 ms
-------------------------------------------
you go the error way


5.打开浏览器
http://localhost:4040/stages/
观察实验结果



这个实验的意思是,从接收的字符串流中打印出带有关键词"error"的语句