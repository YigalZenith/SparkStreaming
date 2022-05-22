package com.imooc.spark.project.spark

import com.imooc.spark.project.dao.{CourseClickCountDao, CourseSearchClickCountDao}
import com.imooc.spark.project.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.imooc.spark.project.utils.DateUtils
import kafka.serializer.StringDecoder
import org.apache.spark.{SPARK_REPO_URL, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object ImoocStatStreamingApp {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.out.println("Usage: ImoocStatStreamingApp <brokers> <topics>")
      System.exit(1)
    }
    System.setProperty("hadoop.home.dir", "C:\\Users\\xxx\\Desktop\\hadoop\\softs\\winutils-master\\hadoop-2.6.0")
    val sparkConf = new SparkConf() //.setMaster("local[2]").setAppName("ImoocStatStreamingApp")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    // 准备 createDirectStream 需要的参数
    val Array(brokers, topics) = args
    val brokersMap = Map("bootstrap.servers" -> brokers)
    val topicsSet = topics.split(",").toSet

    // Spark Streaming对接Kafka
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, brokersMap, topicsSet)

    // 测试步骤一：测试数据接收
    // 返回的消息是一个元组,第二个字段是我们需要的数据
    // lines.map(_._2).count().print()

    // 测试步骤二：数据清洗
    val cleanData = lines.map(_._2)
      // 把每条日志存入 case定义的ClickLog实体类
      .map { info =>
        // 对每一条日志进行切割
        val fields = info.split("\t")

        // 处理时间字段: "2022-05-15 16:01:01" -> 20220515160101
        val newTme = DateUtils.stringToString(fields(1))

        // 从Url字段获取courseId: "GET /class/112.html HTTP/1.1" -> 过滤class开头的courseId
        val url = fields(2).split(" ")(1)
        var courseId = 0
        if (url.startsWith("/class")) {
          val courseIdHTML = url.split("/")(2)
          courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
        }

        // 把获取的信息存入一个对象
        ClickLog(fields(0), newTme, courseId, fields(3).toInt, fields(4))
      }
      // 过滤couseId不为0的日志
      .filter { clickLog => clickLog.CourseID != 0 }

    // 输出过滤后的日志
    // cleanData.print(100)

    // 测试步骤三：统计今天到现在为止实战课程的访问量
    cleanData
      // 创造HBase rowKey:  ("20220515_1",1)
      .map(x => x.TIME.substring(0, 8) + "_" + x.CourseID -> 1)
      // 把本批次数据根据rowKey聚合
      .reduceByKey(_ + _)
      // 把聚合后的数据写入HBase
      .foreachRDD { rdd =>
        // 用分区处理，提升性能
        rdd.foreachPartition { records =>
          val list = ListBuffer[CourseClickCount]()
          records.foreach { record =>
            list.append(CourseClickCount(record._1, record._2))
            // 调用Dao层查询数据
            println(record._1 + "\t" + CourseClickCountDao.count(record._1))
          }
          // 调用Dao层插入数据
          CourseClickCountDao.save(list)
        }
      }

    // 测试步骤四：统计从搜索引擎过来的今天到现在为止实战课程的访问量
    cleanData
      /**
       * https://search.yahoo.com/search?p=Storm实战 处理成=> https:/search.yahoo.com/search?p=Storm实战
       * 然后以/切割成 search.yahoo.com
       */
      .map { x =>
        var host = ""
        val splits = x.Referer.replace("//", "/").split("/")
        if (splits.length > 2) {
          host = splits(1)
        }
        // 把rowkey的字段组成元组
        (x.TIME.substring(0, 8), host, x.CourseID)
        // 过滤掉host为空的数据
      }.filter(_._2 != "")
      // 把剩余的数据构造成map并根据key聚合
      .map( x => x._1+"_"+x._2+"_"+x._3 -> 1).reduceByKey(_ + _)
      // 把聚合后的数据写入HBase
      .foreachRDD { rdd =>
        // 用分区处理，提升性能
        rdd.foreachPartition { records =>
          val list = ListBuffer[CourseSearchClickCount]()
          records.foreach { record =>
            list.append(CourseSearchClickCount(record._1, record._2))
            // 调用Dao层查询数据(先查询后插入,会比库里少一次)
            println(record._1 + "\t" + CourseSearchClickCountDao.count(record._1))
          }
          // 调用Dao层插入数据
          CourseSearchClickCountDao.save(list)
        }
      }

    ssc.start()
    ssc.awaitTermination()
  }
}
