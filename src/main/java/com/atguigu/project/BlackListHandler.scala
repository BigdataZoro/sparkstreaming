package com.atguigu.project

import java.sql.{Connection, Date}
import java.text.SimpleDateFormat

import com.atguigu.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

/**
 * @author jack
 *         Date: 2020-11-25
 *         Time: 19:18
 */
case class Ads_log(timestamp: Long, area: String, city: String, userid: String, adid: String)

object BlackListHandler {
  val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def addBlackList(fileterAdsLogDStream: DStream[Ads_log]): Unit = {
    val dateUserAdToCount: DStream[((String, String, String), Long)] = fileterAdsLogDStream.map(
      adsLog => {
        val date: String = sdf.format(new Date(adsLog.timestamp))
        ((date, adsLog.userid, adsLog.adid), 1L)
      }
    ).reduceByKey(_ + _)
    dateUserAdToCount.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val connection: Connection = JDBCUtil.getConnection
            iter.foreach { case ((dt, user, ad), count) =>
              JDBCUtil.executeUpdate(connection,
                """
                  |insert into user_ad_count (dt,userid,adid,count)
                  |values(?,?,?,?)
                  |on duplicate key
                  |update count=count+?
          """.stripMargin, Array(dt, user, ad, count, count))
              val ct: Long = JDBCUtil.getDataFromMysql(connection,
                """
                  |select count from user_ad_count where dt=? and userid=? and adid = ?
                  |""".stripMargin, Array(dt, user, ad))

              if (ct >= 30) {
                JDBCUtil.executeUpdate(connection,
                  """
                    |insert into black_list (userid) values(?) on duplicate key update userid = ?
                    |""".stripMargin, Array(user, user))
              }
            }
            connection.close()
          }
        )
      }
    )
  }

  def filterByBlackList(adsLogDStream: DStream[Ads_log]): DStream[Ads_log] = {
    adsLogDStream.filter(
      adsLog => {
        val connection: Connection = JDBCUtil.getConnection
        val bool: Boolean = JDBCUtil.isExist(connection,
          """
            |select * from black_list where userid = ?
            |""".stripMargin, Array(adsLog.userid))
        connection.close()
        !bool
      }
    )
  }
}
