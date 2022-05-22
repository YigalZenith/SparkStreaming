package com.imooc.spark.project.domain

/**
 * 实战课程点搜索击数实体类
 * @param day_search_course  对应的就是HBase中的rowkey，20171111_www.baidu.com_1
 * @param click_count 对应的 20171111_www.baidu.com_1 的访问总数
 */
case class CourseSearchClickCount(day_search_course:String,click_count:Long)
