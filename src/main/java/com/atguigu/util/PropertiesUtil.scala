package com.atguigu.util

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author jack
 *         Date: 2020-11-25
 *         Time: 14:06
 */
object PropertiesUtil {
  def load(propertiesName: String): Properties = {
    val prop: Properties = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().
      getContextClassLoader.getResourceAsStream(propertiesName),
      "UTF-8"))
    prop
  }
}
