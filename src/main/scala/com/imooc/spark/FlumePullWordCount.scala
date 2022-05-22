package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Spark Streaming 整合flume的第二种方式 Pull
 */
object FlumePullWordCount {
  def main(args: Array[String]): Unit = {

    if (args.length < 2){
      System.out.println("Usage: FlumePullWordCount <hostname> <port>")
      System.exit(1)
    }

    // 同时赋值多个变量
    val Array(hostname,port) = args

    System.setProperty("hadoop.home.dir", "C:\\Users\\xxx\\Desktop\\hadoop\\softs\\winutils-master\\hadoop-2.6.0")
    val conf = new SparkConf()//.setMaster("local[2]").setAppName("NetworkWC") // 服务器运行是通过参数传递
    val ssc = new StreamingContext(conf, Seconds(5))

    // 获取  ReceiverInputDStream[SparkFlumeEvent]
    val lines = FlumeUtils.createPollingStream(ssc, hostname, port.toInt)

    // 获取内容并进行统计
    lines.map( x => new String(x.event.getBody.array()).trim)
      .flatMap(_.split(" ")).map(_->1).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
