package com.atguigu.project

import java.util.{Properties, Random}

import com.atguigu.util.PropertiesUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.mutable.ArrayBuffer


/**
 * @author jack
 *         Date: 2020-11-25
 *         Time: 14:29
 */
case class CityInfo(city_id: Long, city_name: String, area: String)

object MockerRealTime {

  def generateMockData(): Array[String] = {
    val array: ArrayBuffer[String] = ArrayBuffer[String]()
    val CityRandomOpt: RandomOptions[CityInfo] = RandomOptions(RanOpt(CityInfo(1, "北京", "华北"), 30),
      RanOpt(CityInfo(2, "上海", "华东"), 30),
      RanOpt(CityInfo(3, "广州", "华南"), 10),
      RanOpt(CityInfo(4, "深圳", "华南"), 20),
      RanOpt(CityInfo(5, "天津", "华北"), 10))
    val random: Random = new Random()
    for (i <- 0 to 50) {
      val timestamp: Long = System.currentTimeMillis()
      val cityInfo: CityInfo = CityRandomOpt.getRandomOpt
      val city: String = cityInfo.city_name
      val area: String = cityInfo.area
      val adid: Int = 1 + random.nextInt(6)
      val userid: Int = 1 + random.nextInt(6)
      array += timestamp + " " + area + " " + city + " " + userid + " " + adid
    }
    array.toArray
  }


  def main(args: Array[String]): Unit = {
    val config: Properties = PropertiesUtil.load("config.properties")
    val broker: String = config.getProperty("kafka.broker.list")
    val topic: String = config.getProperty("kafka.topic")

    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val kafkaProducer: KafkaProducer[String, String] =new KafkaProducer[String,String](prop)

    while (true) {
      for (line <- generateMockData()) {
        kafkaProducer.send(new ProducerRecord[String, String](topic, line))
        println(line)
      }
      Thread.sleep(2000)
    }
  }
}
