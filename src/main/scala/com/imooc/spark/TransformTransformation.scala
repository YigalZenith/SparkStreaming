package com.imooc.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Spark Streaming 进行黑名单过滤
 */
object TransformTransformation {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\xxx\\Desktop\\hadoop\\softs\\winutils-master\\hadoop-2.6.0")
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWC")
    val ssc = new StreamingContext(conf, Seconds(5))
    val receiver = ssc.socketTextStream("hadoop000", 8888)

    // 构造黑名单
    val blockList = List("zs", "ls")

    // 把List解析为RDD,并用map把RDD中元素转为元组  RDD[(String, Boolean)] ==> ("zs",true) ("ls",true)
    val spamInfoRDD = ssc.sparkContext.parallelize(blockList).map(_ -> true)

    // 测试数据(通过nc写入)
    // 20180808,zs
    // 20180808,ls
    // 20180808,ww

    // 处理接收的数据
    // 把访问日志 = ReceiverInputDStream[String]
    // 用map把String转为元组  DStream[(String, String)] ==> ("zs","20180808,zs")
    // 用transform中的RDD.leftOuterJoin把元素换为 RDD[(K, (V, Option[W]))] ==> ("zs",("20180808,zs","true"))
    // RDD.filter 把对上面进行过滤,返回值为true的不过滤, 此时transform的返回值为DStream[(String, (String, Option[Boolean]))]
    // 用map把上面结果转为 DStream[String] ==> "20180808,ww",也可以用map处理RDD数据(即RDD.filter().map,老师用的此方法)
    val result = receiver.map(x => x.split(",")(1) -> x).transform{ rdd =>
      rdd.leftOuterJoin(spamInfoRDD).filter { rdd2 =>
        ! rdd2._2._2.getOrElse(false)
      }
    }.map(_._2._1)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
