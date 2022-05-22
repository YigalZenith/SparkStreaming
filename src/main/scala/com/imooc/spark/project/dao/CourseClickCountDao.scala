package com.imooc.spark.project.dao

import com.imooc.spark.project.domain.CourseClickCount
import com.imooc.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
 * 实战课程点击数-数据访问层
 */
object CourseClickCountDao {
  val tableName = "imooc_course_clickcount"
  val cf = "info"
  val qualifer = "c1"

  /**
   * 保存数据到HBase
   * @param list  CourseClickCount集合
   */
  def save(list: ListBuffer[CourseClickCount]): Unit = {
    val hTable = HBaseUtils.getInstance().getTable(tableName)

    for (i <- list) {
      hTable.incrementColumnValue(
        i.day_course.getBytes,
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

    // 根据rowKey查询 info:c1 的 value
    // val l = count("20171111_1")
    // println(l)

    val list = ListBuffer[CourseClickCount](
      CourseClickCount("20220515_1", 111.toLong),
      CourseClickCount("20220515_2", 222.toLong),
      CourseClickCount("20220515_3", 333.toLong)
    )
    save(list)

    println(count("20220515_1") + ":" + count("20220515_2") + ":" + count("20220515_3"))
  }
}
