package com.atguigu.saprkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jack
 *         Date: 2020-11-24
 *         Time: 10:41
 */
object SparkStreaming_WordCount {
  def main(args: Array[String]): Unit = {
    //编写配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    //构建环境
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))
    //连接主机
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop106", 9999)
    //读取一行数据进行切分
    val wordDStream: DStream[String] = lineDStream.flatMap(_.split("_"))
    //转换形态，方便聚合（v => k-v）
    val wordToOneDstream: DStream[(String, Int)] = wordDStream.map((_, 1))
    //sum聚合value
    val wordToSumDStream: DStream[(String, Int)] = wordToOneDstream.reduceByKey(_ + _)
    //输出聚合的结果
    wordToSumDStream.print()
    //启动streaming
    ssc.start()
    //阻塞线程
   ssc.awaitTermination()
  }
}
