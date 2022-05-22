package com.imooc.spark.project.domain

/**
 * 用来保存一条清洗后日志的实体类
 * @param IP 访问者IP
 * @param TIME 访问时间
 * @param CourseID 访问的课程ID
 * @param Status 访问状态码
 * @param Referer 访问跳转源
 */
case class ClickLog(IP:String,TIME:String,CourseID:Int,Status:Int,Referer:String)
