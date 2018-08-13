package com.scalalearn.scala.main;
import scopt.OptionParser
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream._
import com.scalalearn.java.main.ApacheAccessLog


/**
 * The LogAnalyzerAppMain is an sample logs analysis application.  For now,
 * it is a simple minimal viable product:
 *   - Read in new log files from a directory and input those new files into streaming.
 *   - Computes stats for all of time as well as the last time interval based on those logs.
 *   - Write the calculated stats to an txt file on the local file system
 *     that gets refreshed every time interval.
 *
 * Once you get this program up and running, feed apache access log files
 * into the local directory of your choosing.
 *
 * Then open your output text file, perhaps in a web browser, and refresh
 * that page to see more stats come in.
 *
 * Modify the command line flags to the values of your choosing.
 * Notice how they come after you specify the jar when using spark-submit.
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.oreilly.learningsparkexamples.scala.logs.LogAnalyzerAppMain"
 *     --master local[4]
 *     target/uber-log-analyzer-1.0.jar
 *     --logs_directory /tmp/logs
 *     --output_html_file /tmp/log_stats.html
 *     --index_html_template ./src/main/resources/index.html.template
 */
case class Config(WindowLength: Int = 1000, SlideInterval: Int = 3000, LogsDirectory: String = "/tmp/logs",
  CheckpointDirectory: String = "/tmp/checkpoint",
  OutputHTMLFile: String = "/tmp/log_stats.html",
  OutputDirectory: String = "/tmp/outpandas",
  IndexHTMLTemplate :String ="./src/main/resources/index.html.template") 
{
  def getWindowDuration() = 
  {
    new Duration(WindowLength)
  }
  def getSlideDuration() = 
  {
    new Duration(SlideInterval)
  }
}

object LogAnalyzerAppMain 
{

  def main(args: Array[String]) 
  {
    val parser = new scopt.OptionParser[Config]("LogAnalyzerAppMain")
    {
      head("LogAnalyzer", "0.1")
      opt[Int]('w', "window_length") text("size of the window as an integer in miliseconds")
      opt[Int]('s', "slide_interval") text("size of the slide inteval as an integer in miliseconds")
      opt[String]('l', "logs_directory") text("location of the logs directory. if you don't have any logs use the fakelogs_dir script.")
      opt[String]('c', "checkpoint_directory") text("location of the checkpoint directory.")
      opt[String]('o', "output_directory") text("location of the output directory.")
    }
    val opts = parser.parse(args, new Config()).get
    // Startup the Spark Conf.
    val conf = new SparkConf()
      .setAppName("A Databricks Reference Application: Logs Analysis with Spark")


    val ssc = new StreamingContext(conf, opts.getWindowDuration())

    // Checkpointing must be enabled to use the updateStateByKey function & windowed operations.
    ssc.checkpoint(opts.CheckpointDirectory)
    // This methods monitors a directory for new files to read in for streaming.
    val logDirectory = opts.LogsDirectory
    println("opts.LogsDirectory="+opts.LogsDirectory)

    //从下面开始接收数据！！！

    val logData = ssc.textFileStream(logDirectory);//注意，这里会遇到无法读取数据的问题，根据下面的参考链接，
//    想要读取数据，首先必须是文件夹作为参数，不能是具体文件名作为参数，另外该文件必须是.txt后缀，否则也获取不到数据
//    参考链接：
    //http://www.aboutyun.com/thread-17528-2-1.html
    println("logData="+logData.print())//用来判断数据到底进来了没有

    //最终我们会发现，这个地方什么数据都没有，因为我们放上去的数据他是不认的，他只认可＂流数据＂，不认＂静态数据＂
//    可以参考以下连接
//    https://blog.csdn.net/young_so_nice/article/details/51629049



    println("logData type="+logData.getClass.getSimpleName())
    val accessLogDStream = logData.map(line => ApacheAccessLog.parseFromLogLine(line)).cache()
    println("accessLogDSTream:"+accessLogDStream)
    LogAnalyzerTotal.processAccessLogs(accessLogDStream)
    LogAnalyzerWindowed.processAccessLogs(accessLogDStream, opts)
  }
}

//没有结果，可能的原因是１，没有读进去，２，没有返回．
