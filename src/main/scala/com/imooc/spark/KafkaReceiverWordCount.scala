package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Spark Streaming 整合kafka的第一种方式 Receiver
 */
object KafkaReceiverWordCount {
  def main(args: Array[String]): Unit = {

    if (args.length < 4){
      System.out.println("Usage: KafkaReceiverWordCount <zkAddress> <groupId> <topics> <partitionId>")
      System.exit(1)
    }

    // 同时赋值多个变量
    val Array(zkAddress,groupId,topics,partitionId) = args
    val topicMap = topics.split(",").map(_ -> partitionId.toInt).toMap

    System.setProperty("hadoop.home.dir", "C:\\Users\\xxx\\Desktop\\hadoop\\softs\\winutils-master\\hadoop-2.6.0")
    val conf = new SparkConf() //.setMaster("local[2]").setAppName("NetworkWC") // 服务器运行是通过参数传递
    val ssc = new StreamingContext(conf, Seconds(5))

    // Spark Streaming对接Kafka
    val lines = KafkaUtils.createStream(ssc, zkAddress, groupId, topicMap)
    // 返回的消息是一个元组,第二个字段是我们需要的数据
    lines.flatMap(_._2.split(" ")).map(_->1).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
