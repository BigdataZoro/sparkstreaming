package com.atguigu.saprkstreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author jack
 *         Date: 2020-11-24
 *         Time: 11:35
 */
object SparkStreaming_CustomerReceiver {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val lineDStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("hadoop102", 9999))
    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))
    val wordToSumDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_ + _)
    wordToSumDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
  class CustomerReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
    override def onStart(): Unit = {
      new Thread("Socket Reciver"){

        override def run(): Unit ={
          receive()
        }
      }.start()
    }

    def receive(): Unit = {
      val socket: Socket = new Socket(host, port)
      val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
      var input: String = reader.readLine()
      while (!isStopped() && input !=null){
        store(input)
        input = reader.readLine()
      }
      reader.close()
      socket.close()
      restart("restart")
    }
    override def onStop(): Unit = {}
  }
}
