package com.imooc.spark.offset.utils

import kafka.common.TopicAndPartition

import scala.collection.JavaConversions._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.streaming.kafka.OffsetRange

import scala.collection.mutable

object ZkUtils {
    // ZK client
    val client = {
      val client = CuratorFrameworkFactory
        .builder
        .connectString("hadoop000:2181")
        .retryPolicy(new ExponentialBackoffRetry(1000, 3))
        .namespace("mykafka")
        .build()
      client.start()
      client
    }

    // offset 路径起始位置
    val Globe_kafkaOffsetPath = "/kafka/offsets"

    // 路径确认函数  确认ZK中路径存在，不存在则创建该路径
    def ensureZKPathExists(path: String): Any ={
      if (client.checkExists().forPath(path) == null) {
        client.create().creatingParentsIfNeeded().forPath(path)
      }
    }

    // 保存 新的 offset
    def storeOffsets(offsetRange: Array[OffsetRange], groupName:String): Unit = {
      for (o <- offsetRange){
        val zkPath = s"${Globe_kafkaOffsetPath}/${groupName}/${o.topic}/${o.partition}"

        // 向对应分区第一次写入或者更新Offset 信息
        println("---Offset写入ZK------\nTopic：" + o.topic +", Partition:" + o.partition + ", Offset:" + o.untilOffset)

        // 检查路径是否存在
        ensureZKPathExists(zkPath)

        client.setData().forPath(zkPath, o.untilOffset.toString.getBytes())
      }
    }

    def getFromOffset(topic: Array[String], groupName:String):(Map[TopicAndPartition, Long], Int) = {

      // Kafka 0.8和0.10的版本差别，0.10 为 TopicPartition   0.8 TopicAndPartition
      var fromOffset: Map[TopicAndPartition, Long] = Map()

      val topic1 = topic(0).toString

      // 读取ZK中保存的Offset，作为Dstrem的起始位置。如果没有则创建该路径，并从 0 开始Dstream
      val zkTopicPath = s"${Globe_kafkaOffsetPath}/${groupName}/${topic1}"

      // 检查路径是否存在
      ensureZKPathExists(zkTopicPath)

      // 获取topic的子节点，即 分区
      val childrens = client.getChildren().forPath(zkTopicPath)

      // 遍历分区
      val offSets: mutable.Buffer[(TopicAndPartition, Long)] = for {
        p <- childrens
      }
      yield {
        // 遍历读取子节点中的数据：即 offset
        val offsetData = client.getData().forPath(s"$zkTopicPath/$p")
        // 将offset转为Long
        val offSet = java.lang.Long.valueOf(new String(offsetData)).toLong
        // 返回  (TopicAndPartition, Long)
        (new TopicAndPartition(topic1, Integer.parseInt(p)), offSet)
      }

      // 初始时打印map
//      println(offSets.toMap)
      if(offSets.isEmpty){
        (offSets.toMap, 0)
      } else {
        (offSets.toMap, 1)
      }
    }

}
