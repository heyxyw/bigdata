package com.zhouq.spark.ser

import java.net.InetAddress

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 序列化测试
  */
object SerDemo {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SerDemo")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))

    // 第二种方式: Driver 端中产生规则对象,下面的函数引用,也叫闭包.
    // 这种方式在Executor 中的每个Task 中的Rules对象都不一样(在Task 内部是同一个,但是在同一个 Executor 中的task 是不同的).
    //    val rules = new Rules

    //第三种方式:
    //    val rules1 = new Rules1

    val result: RDD[(String, String, Double, String)] = lines.map(word => {

      val hostName: String = InetAddress.getLocalHost.getHostName

      val threadName: String = Thread.currentThread().getName

      //第一种方式: 这种方式会产生大量的规则对象,影响性能
      //val rules = new Rules

      //函数的执行是在Executor执行的（Task中执行的）
      // 第四种方式: 直接使用在Executor 中使用 Rules1.rulesMap, 无需从Driver 端传入规则.
      (hostName, threadName, Rules1.rulesMap.getOrElse(word, 0), Rules1.toString)

      //第三种方式:
      //(hostName, threadName, rules1.rulesMap.getOrElse(word, 0), rules1.toString)

    })

    result.saveAsTextFile(args(1))

    sc.stop()

  }

}
