package com.imooc.spark.offset

import com.imooc.spark.offset.utils.ZkUtils
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 通过把offset保存到zk实现
 * 1.创建StreamingContext
 * 2.从kafka中获取数据 ===> offset获取
 * 3.根据业务逻辑进行处理
 * 4.将结果写到外部存储中去 ===> offset保存
 * 5.启动程序，等待程序终止
 * 参考: https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
 * 从zk读写offset参考: https://blog.csdn.net/liaomingwu/article/details/123182513
 */
object OffsetApp03 {
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

    // 从zk获取offset(只读取一次)
    val topicsArray = "streaming_offset".split(",").toArray
    val groupId = "test"
    // fromOffsets: 保存offset的map
    // flag: 0代表从头消费,1则从zk获取的offset处继续消费
    val (fromOffsets, flag) = ZkUtils.getFromOffset(topicsArray, groupId)
    println(fromOffsets,flag)

    // Spark Streaming对接Kafka
    val messages =
      if (flag == 0) { // 从头开始消费
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, brokersMap, topicsSet)
      } else { // 从指定偏移量开始
        // 用于将每条消息和元数据转换为所需类型的函数,这里把key和message组成元组
        val messageHandler = (x: MessageAndMetadata[String, String]) => (x.key(), x.message())
        // 从指定偏移量开始
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, brokersMap, fromOffsets, messageHandler)
      }

    messages.foreachRDD { rdd =>
      // 假设业务逻辑是打印
      println("消息数量: " + rdd.count())

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // 保存前输出offset(每个批次都会执行保存)
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
      // 将offset保存到zk
      ZkUtils.storeOffsets(offsetRanges, "test")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
