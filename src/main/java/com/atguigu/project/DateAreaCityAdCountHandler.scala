package com.atguigu.project

import java.sql.{Connection, Date}
import java.text.SimpleDateFormat

import com.atguigu.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

/**
 * @author jack
 *         Date: 2020-11-25
 *         Time: 20:34
 */
object DateAreaCityAdCountHandler {

  val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def saveDateAreaCityAdCountToMysql(filterAdsLogDStream: DStream[Ads_log]): Unit = {
    val dataAreaCityAdToCount: DStream[((String, String, String, String), Long)] = filterAdsLogDStream.map(ads_log => {
      val dt: String = sdf.format(new Date(ads_log.timestamp))
      ((dt, ads_log.area, ads_log.city, ads_log.adid), 1L)
    }).reduceByKey(_ + _)
    dataAreaCityAdToCount.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val connection: Connection = JDBCUtil.getConnection
        iter.foreach {
          case ((dt, area, city, adid), ct) =>
            JDBCUtil.executeUpdate(connection,
              """
                |inster into area_city_ad_count (dt,area,city,adid,count)
                | values(?,?,?,?,?)
                | on duplicate key
                | update count = count+?
                |""".stripMargin, Array(dt, area, city, adid, ct, ct))
        }
        connection.close()
      })
    })
  }
}
