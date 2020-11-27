package com.atguigu.saprkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author jack
 *         Date: 2020-11-24
 *         Time: 18:46
 */
object SparkStreaming_ReduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_ReduceByKeyAndWindow")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./ck")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop106", 9999)
    val wordToOne: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))
    val wordCount: DStream[(String, Int)] = wordToOne.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(12), Seconds(6))
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
