package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, DriverManager}

/**
 * 使用Spark Streaming完成词频统计，并将结果写入到MySQL数据库中
 */
object ForeachRDDTransformation {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\xxx\\Desktop\\hadoop\\softs\\winutils-master\\hadoop-2.6.0")
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWC")
    val ssc = new StreamingContext(conf, Seconds(5))

    val receiver = ssc.socketTextStream("hadoop000", 8888)
    val result = receiver.flatMap(_.split(" ")).map(_ -> 1).reduceByKey(_ + _)

    //result.print()
    // 用ForeachRDD方法把统计结果保存到MySQL
    result.foreachRDD { rdd =>
      rdd.foreachPartition { records =>
        val connection = createConnection()
        records.foreach { record =>
          val sql = "insert into wordcount(word, wordcount) values('" + record._1+"',"+record._2 + ")"
          connection.createStatement().execute(sql)
        }
        connection.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def createConnection(): Connection = {
    // 加载驱动(开发推荐的方式)
    Class.forName("com.mysql.jdbc.Driver")
    // 获取连接,隐式返回
    DriverManager.getConnection("jdbc:mysql://hadoop000:3306/imooc_spark","root","root")
  }
}
