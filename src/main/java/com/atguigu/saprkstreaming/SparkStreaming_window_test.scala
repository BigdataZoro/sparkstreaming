package com.atguigu.saprkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author jack
 *         Date: 2020-11-25
 *         Time: 9:20
 */
object SparkStreaming_window_test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_window_test")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    val linesDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop106", 9999)
    val wordToOneDS: DStream[(String, Int)] = linesDS.flatMap(_.split(" ")).map((_, 1))
    val wordByWindow: DStream[(String, Int)] = wordToOneDS.window(Seconds(6), Seconds(6))
    val wordToSumDS: DStream[(String, Int)] = wordByWindow.reduceByKey(_ + _)
    wordToSumDS.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
