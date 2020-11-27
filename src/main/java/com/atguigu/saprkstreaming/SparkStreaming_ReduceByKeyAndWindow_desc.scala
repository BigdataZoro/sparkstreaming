package com.atguigu.saprkstreaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @author jack
 *         Date: 2020-11-24
 *         Time: 18:52
 */
object SparkStreaming_ReduceByKeyAndWindow_desc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_ReduceByKeyAndWindow_desc")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./ck")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop106", 9999)
    val wordToOne: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))
    val wordToSumDStream: DStream[(String, Int)] = wordToOne.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), (x: Int, y: Int) => (x - y), Seconds(12), Seconds(6), new HashPartitioner(2), (x: (String, Int)) => x._2 > 0)
    wordToSumDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
