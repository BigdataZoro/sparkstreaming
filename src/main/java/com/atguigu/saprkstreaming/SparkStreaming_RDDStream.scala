package com.atguigu.saprkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author jack
 *         Date: 2020-11-24
 *         Time: 11:24
 */
object SparkStreaming_RDDStream {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(4))
    val rddQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()
    val inputDStream: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)
    val sumDStream: DStream[Int] = inputDStream.reduce(_ + _)
    sumDStream.print()
    ssc.start()
    for (i <- 1 to 5){
      rddQueue += ssc.sparkContext.makeRDD(1 to 5)
      Thread.sleep(2000)
    }
    ssc.awaitTermination()
  }
}
