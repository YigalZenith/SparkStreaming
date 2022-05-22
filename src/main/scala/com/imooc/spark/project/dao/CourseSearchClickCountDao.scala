package com.imooc.spark.project.dao

import com.imooc.spark.project.domain.CourseSearchClickCount
import com.imooc.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
 * 实战课程搜索点击数-数据访问层
 */
object CourseSearchClickCountDao {
  val tableName = "imooc_course_search_clickcount"
  val cf = "info"
  val qualifer = "c1"

  /**
   * 保存数据到HBase
   * @param list  CourseSearchClickCount集合
   */
  def save(list: ListBuffer[CourseSearchClickCount]): Unit = {
    val hTable = HBaseUtils.getInstance().getTable(tableName)

    for (i <- list) {
      hTable.incrementColumnValue(
        i.day_search_course.getBytes,
        cf.getBytes,
        qualifer.getBytes,
        i.click_count
      )
    }
  }

  /**
   * 根据 rowKey 查询值
   */
  def count(rowKey: String): Long = {
    val hTable = HBaseUtils.getInstance().getTable(tableName)
    val hGet = new Get(Bytes.toBytes(rowKey))
    val value = hTable.get(hGet).getValue(cf.getBytes, qualifer.getBytes)
    if (value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\xxx\\Desktop\\hadoop\\softs\\winutils-master\\hadoop-2.6.0")

    val list = ListBuffer[CourseSearchClickCount](
      CourseSearchClickCount("20220515_www.baidu.com_1", 1.toLong),
      CourseSearchClickCount("20220515_www.binging.com_2", 2.toLong)
    )
    save(list)

    println(count("20220515_www.baidu.com_1") + ":" + count("20220515_www.binging.com_2"))
  }
}
