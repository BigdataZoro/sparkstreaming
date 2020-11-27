package com.atguigu.util

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

/**
 * @author jack
 *         Date: 2020-11-25
 *         Time: 17:51
 */
object JDBCUtil {

  var dataSource : DataSource = init()
  def init():DataSource ={
    val properties: Properties = new Properties()
    val config: Properties = PropertiesUtil.load("config.properties")
    properties.setProperty("driverClassName","com.mysql.jdbc.Driver")
    properties.setProperty("url",config.getProperty("jdbc.url"))
    properties.setProperty("username",config.getProperty("jdbc.user"))
    properties.setProperty("password",config.getProperty("jdbc.password"))
    properties.setProperty("maxActive",config.getProperty("jdbc.datasource.size"))
    DruidDataSourceFactory.createDataSource(properties)
  }

  def getConnection:Connection ={
    dataSource.getConnection
  }
  def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int = {
    var rtn = 0
    var pstmt: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      connection.commit()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {
    var flag: Boolean = false
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      flag = pstmt.executeQuery().next()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    flag
  }

  def getDataFromMysql(connection: Connection, sql: String, params: Array[Any]): Long = {
    var result: Long = 0L
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      val resultSet: ResultSet = pstmt.executeQuery()
      while (resultSet.next()) {
        result = resultSet.getLong(1)
      }
      resultSet.close()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result
  }

  def main(args: Array[String]): Unit = {
    val connection: Connection = getConnection
    val statement: PreparedStatement = connection.prepareStatement("select * from user_ad_count where userid = ?")
    statement.setObject(1,"a")
    val resultSet: ResultSet = statement.executeQuery()
    while (resultSet.next()){
      println("1111:"+resultSet.getString(1))
    }
    resultSet.close()
    statement.close()
    connection.close()
  }
}
