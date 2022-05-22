package com.imooc.spark

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Spark Streaming 整合kafka的第二种方式 Direct
 */
object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {

    if (args.length < 2){
      System.out.println("Usage: KafkaDirectWordCount <brokers> <topics>")
      System.exit(1)
    }

    // 同时赋值多个变量
    val Array(brokers,topics) = args

    // 准备对接kafka需要的参数
    val kafkaParams = Map("bootstrap.servers"-> brokers)
    val topicSet = topics.split(",").toSet

    System.setProperty("hadoop.home.dir", "C:\\Users\\xxx\\Desktop\\hadoop\\softs\\winutils-master\\hadoop-2.6.0")
    val conf = new SparkConf() //.setMaster("local[2]").setAppName("NetworkWC") // 服务器运行是通过参数传递
    val ssc = new StreamingContext(conf, Seconds(5))

    // Spark Streaming对接Kafka
    val lines = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topicSet)

    // 返回的消息是一个元组,第二个字段是我们需要的数据
    lines.flatMap(_._2.split(" ")).map(_->1).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
