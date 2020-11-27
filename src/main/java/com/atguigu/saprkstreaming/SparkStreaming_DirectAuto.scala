package com.atguigu.saprkstreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author jack
 *         Date: 2020-11-24
 *         Time: 16:20
 */
object SparkStreaming_DirectAuto {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_DirectAuto")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val kafkaPara: Map[String, Object] = Map[String, Object](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop106:9092,hadoop107:9092,hadoop108:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguiguGroup",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])

    val kafkaDStream: InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String,String](Set("testTopic"), kafkaPara))
    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())
    valueDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
