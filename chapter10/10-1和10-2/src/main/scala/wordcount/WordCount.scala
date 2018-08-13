/**
 * Illustrates a simple streaming application
 */
// package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

object WordCount
{



  def main(args: Array[String]) 
  {
    // val master = args(0)
    val conf = new SparkConf().setMaster("local[4]").setAppName("StreamingLogInput")
    // Create a StreamingContext with a 1 second batch size
    val ssc = new StreamingContext(conf, Seconds(1))
    // Create a DStream from all the input on port 9999
    val lines = ssc.socketTextStream("localhost", 9999)
    val errorLines = processLines(lines)
    // Print out the lines with errors, which causes this DStream to be evaluated
    errorLines.print()
    // start our streaming context and wait for it to "finish"
    ssc.start()
    // Wait for 10 seconds then exit. To run forever call without a timeout
    ssc.awaitTermination()//note that don't write as ssc.awaitTermination(10000),that's wrong
    ssc.stop()
  }



  def processLines(lines: DStream[String]) = {
    // Filter our DStream for lines with "error"
    lines.filter(_.contains("error"))
  }

}
