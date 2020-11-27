package com.atguigu.saprkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author jack
 *         Date: 2020-11-24
 *         Time: 18:16
 */
object SparkStreaming_updateStateByKey {
  val updateFunc = (seq:Seq[Int],state:Option[Int]) =>{
    val currentCount: Int = seq.sum
    val previousCount: Int = state.getOrElse(0)
    Some(currentCount + previousCount)
  }
  def createSCC():StreamingContext = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./ck")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop106", 9999)
    val word: DStream[String] = lines.flatMap(_.split(" "))
    val wordToOne: DStream[(String, Int)] = word.map(word => (word, 1))
    val stateDSteam: DStream[(String, Int)] = wordToOne.updateStateByKey[Int](updateFunc)
    stateDSteam.print()
    ssc
  }
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck", () => createSCC())
    ssc.start()
    ssc.awaitTermination()
  }
}
