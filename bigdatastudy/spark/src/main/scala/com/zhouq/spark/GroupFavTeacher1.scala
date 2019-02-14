package com.zhouq.spark

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求每门课程最受欢迎老师TopN   --1
  *
  */
object GroupFavTeacher1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")

    //创建spark 执行入口
    val sc = new SparkContext(conf)

    //指定读取数据
    val lines: RDD[String] = sc.textFile(args(0))

    val subjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      var teacher = line.substring(index + 1)
      var httpHost = line.substring(0, index)
      var subject = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })

    //将学科,老师联合当做key
    val reduced: RDD[((String, String), Int)] = subjectTeacherAndOne.reduceByKey(_ + _)

    //分组排序(按学科进行排序)
    //[学科,该学科对应的老师的数据]
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)

    /**
      * 分组后,一个分区可能有多个学科的数据,一个学科就是一个迭代器
      * 将每一个分区的数据拿出来进行操作
      * 下面为什么可以使用scala 的sortBy 方法,是因为数据全部已经在一个机器上的scala 集合里面了.
      */

    val sorted = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(3))

    //收集计算结果
    val result = sorted.collect()

    println(result.toBuffer)

    sc.stop()
  }
}
