package com.atguigu.project

import java.util.Properties

import com.atguigu.util.{MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author jack
 *         Date: 2020-11-25
 *         Time: 20:08
 */
object RealTimeApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealTimeApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic: String = properties.getProperty("kafka.topic")
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.geiKafkaStream(topic, ssc)
    val adsLogDStream: DStream[Ads_log] = kafkaDStream.map(record => {
      val value: String = record.value()
      val arr: Array[String] = value.split(" ")
      Ads_log(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
    })

    val filterAdsLogDStream: DStream[Ads_log] = BlackListHandler.filterByBlackList(adsLogDStream)
    BlackListHandler.addBlackList(filterAdsLogDStream)
    filterAdsLogDStream.cache()
    filterAdsLogDStream.count().print()

//    DateAreaCityAdCountHandler.saveDateAreaCityAdCountToMysql(filterAdsLogDStream)

    ssc.start()
    ssc.awaitTermination()
  }
}
