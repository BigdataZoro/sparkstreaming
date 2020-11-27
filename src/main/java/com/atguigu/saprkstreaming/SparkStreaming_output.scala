package com.atguigu.saprkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author jack
 *         Date: 2020-11-24
 *         Time: 19:22
 */
object SparkStreaming_output {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_output")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    val linesDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop106", 9999)
    val wordToOneDStream: DStream[(String, Int)] = linesDStream.flatMap(_.split(" ")).map((_, 1))
    wordToOneDStream.foreachRDD(rdd=>{
      println("22222"+Thread.currentThread().getName)
      rdd.foreachPartition(iter=>iter.foreach(println))
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
