package com.atguigu.saprkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author jack
 *         Date: 2020-11-24
 *         Time: 17:42
 */
object SparkStreaming_Transform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_Transform")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop106", 9999)
    println("11111"+Thread.currentThread().getName)
    val wordToSumDStream: DStream[(String, Int)] = lineDStream.transform(
      rdd => {
        println("22222"+Thread.currentThread().getName)
        val word: RDD[String] = rdd.flatMap(_.split(" "))
        val wordToOne: RDD[(String, Int)] = word.map(x => {
          println("33333"+Thread.currentThread().getName)
          (x, 1)
        })
        val value: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
        value
      }
    )
    wordToSumDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
