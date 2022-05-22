package com.imooc.spark.project.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
 * 日期时间工具类
 */
object DateUtils {

  val SOURCE_FROMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGE_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")

  // 先把时间字符串转为Long类型
  def stringToLong(time: String) = {
    // FastDateFormat 先解析为 Date ,然后转为 Long
    SOURCE_FROMAT.parse(time).getTime
  }

  // 使用Long类型创建一个Date类型对象, 再把Date对象格式化成目标字符串
  def stringToString(time :String) = {
    // 使用Long类型创建一个Date类型对象
    val date = new Date(stringToLong(time))
    // 把Date对象格式化成目标字符串
    TARGE_FORMAT.format(date)
  }

  def main(args: Array[String]): Unit = {
    println(stringToString("2017-10-22 14:46:01"))
  }
}
