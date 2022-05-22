package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 使用Spark Streaming完成有状态统计
 */
object StatefulWordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\xxx\\Desktop\\hadoop\\softs\\winutils-master\\hadoop-2.6.0")

    val sc = new SparkConf().setMaster("local[2]").setAppName("StatefulWordCount")
    val ssc = new StreamingContext(sc, Seconds(5))

    // 如果使用了stateful的算子，必须要设置checkpoint
    // 在生产环境中，建议大家把checkpoint设置到HDFS的某个文件夹中
    ssc.checkpoint("./ssctest")

    val lines = ssc.socketTextStream("hadoop000", 8888)
    val pairs = lines.flatMap(_.split(" ")).map(_ -> 1)
    // 传递的是函数,新版Idea会隐士把方法转换为函数,如果报错需要显示转换(updateFunction _)
    val result = pairs.updateStateByKey(updateFunction)
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 用新接收的数据持续更新老的数据,这里是求和
   * @param newValues 单词新出现的次数,保存到Seq[int],如 "hadoop" -> Seq[Int](1, 1),"spark" -> Seq[Int](1, 1, 1)
   * @param preVaules 单词已经出现过的次数,保存到Option[Int],如 "hadoop" -> Some(3), "spark" -> Some(5)
   * @return
   */
  def updateFunction(newValues: Seq[Int], preVaules: Option[Int]): Option[Int] = {
    // 对新出现的每个单词的次数求和
    val newCount = newValues.sum
    // 获取已出现单词的次数
    val preCount = preVaules.getOrElse(0)
    // 两者相加,保存到Some[Int]中
    Some(newCount+preCount)
  }
}
