package com.zhouq.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求最受欢迎的老师
  *
  */
object FavTeacher {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")
    //创建spark 执行入口

    val sc = new SparkContext(conf)

    //指定读取数据
    val lines: RDD[String] = sc.textFile(args(0))

    val teacherAndOne = lines.map(line => {
      val index = line.lastIndexOf("/")
      var teacher = line.substring(index + 1)
      (teacher, 1)
    })

    //聚合
    val reduced = teacherAndOne.reduceByKey(_ + _)

    //排序
    val sorted = reduced.sortBy(_._2, false)

    //触发Action 计算
    val result = sorted.collect()

    //打印
    println(result.toBuffer)

    Thread.sleep(30000)
    //关闭资源
//    sc.stop()
  }
}
