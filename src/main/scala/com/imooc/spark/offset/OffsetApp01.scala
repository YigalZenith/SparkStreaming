package com.imooc.spark.offset

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 每次从头消费kafka中数据
 */
object OffsetApp01 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\xxx\\Desktop\\hadoop\\softs\\winutils-master\\hadoop-2.6.0")
    val sparkConf: SparkConf = new SparkConf().setAppName("OffsetApp01").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // 准备 createDirectStream 需要的参数
    val brokersMap = Map(
      "bootstrap.servers" -> "hadoop000:9092",
      "auto.offset.reset" -> "smallest"
    )
    val topicsSet = "streaming_offset".split(",").toSet

    // Spark Streaming对接Kafka
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, brokersMap, topicsSet)
    messages.foreachRDD(rdd =>
      if (!rdd.isEmpty()) {
        println("消息数量: " + rdd.count())
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
