先把json转化为parquet文件放到hdfs文件系统中,
然后再运行对parquet文件的读写
参考链接:
http://blog.sina.com.cn/s/blog_4b1452dd0102x2af.html




具体实验步骤:
一,json文件上传到hdfs系统
hdfs dfs -copyFromLocal people.json /user/appleyuchi

二,运行example_prepare.py
把json文件转化为parquet文件.
因为作者的github没有提供.parquet文件,所以没办法,我们要运行上面两步骤,来自己生成parquet文件


三,运行example9-18~9-20.py
读取HDFS系统上的parquet文件中的内容.



