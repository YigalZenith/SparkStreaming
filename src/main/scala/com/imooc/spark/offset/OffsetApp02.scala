package com.imooc.spark.offset

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 用RDD checkpoint实现连续的数据消费
 * 缺点: 修改代码后，新代码无法生效
 * 参考: https://spark.apache.org/docs/2.2.0/streaming-programming-guide.html#how-to-configure-checkpointing
 */
object OffsetApp02 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\xxx\\Desktop\\hadoop\\softs\\winutils-master\\hadoop-2.6.0")
    System.setProperty("HADOOP_USER_NAME","hadoop")
    val sparkConf: SparkConf = new SparkConf().setAppName("OffsetApp01").setMaster("local[2]")

    // 准备 createDirectStream 需要的参数
    val brokersMap = Map(
      "bootstrap.servers" -> "hadoop000:9092",
      "auto.offset.reset" -> "smallest"
    )

    val topicsSet = "streaming_offset".split(",").toSet

    val checkpointDirectory = "hdfs://hadoop000:8020/offsets"

    // Function to create and setup a new StreamingContext
    def functionToCreateContext(): StreamingContext = {
      val ssc = new StreamingContext(sparkConf, Seconds(10))

      // Spark Streaming对接Kafka
      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, brokersMap, topicsSet)

      ssc.checkpoint(checkpointDirectory)   // set checkpoint directory
      messages.checkpoint(Seconds(10))
      messages.foreachRDD(rdd =>
        if (!rdd.isEmpty()) {
          println("消息数量: " + rdd.count())
        }
      )
      ssc  // 返回ssc
    }

    // Get StreamingContext from checkpoint data or create a new one
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)

    ssc.start()
    ssc.awaitTermination()
  }
}
