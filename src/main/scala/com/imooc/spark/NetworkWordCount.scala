package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Spark Streaming 处理Socket数据
 */
object NetworkWordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\xxx\\Desktop\\hadoop\\softs\\winutils-master\\hadoop-2.6.0")
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWC")
    val ssc = new StreamingContext(conf, Seconds(5))
    val receiver = ssc.socketTextStream("hadoop000", 8888)
    val result = receiver.flatMap(_.split(" ")).map(_ -> 1).reduceByKey(_ + _)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
