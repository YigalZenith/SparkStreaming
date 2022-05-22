package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileWordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\xxx\\Desktop\\hadoop\\softs\\winutils-master\\hadoop-2.6.0")

    // 创建context
    val sc = new SparkConf().setMaster("local[2]").setAppName("FileWorkCount")
    val ssc = new StreamingContext(sc, Seconds(5))

    // 设置要监听的目录
    //    val value = ssc.textFileStream("D:/ssc/")
    val value = ssc.textFileStream("hdfs://hadoop000:8020/ssctest/")

    // 处理数据并输出结果
    val result = value.flatMap(_.split(" ")).map(_ -> 1).reduceByKey(_ + _)
    result.print()

    // 启动
    ssc.start()
    ssc.awaitTermination()
  }
}
