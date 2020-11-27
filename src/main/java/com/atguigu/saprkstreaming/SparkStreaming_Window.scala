package com.atguigu.saprkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author jack
 *         Date: 2020-11-24
 *         Time: 18:36
 */
object SparkStreaming_Window {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_Window")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop106", 9999)
    val wordToOneDStream: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))
    val wordToByWindow: DStream[(String, Int)] = wordToOneDStream.window(Seconds(12), Seconds(6))
    val wordToCountDStream: DStream[(String, Int)] = wordToByWindow.reduceByKey(_ + _)
    wordToCountDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
