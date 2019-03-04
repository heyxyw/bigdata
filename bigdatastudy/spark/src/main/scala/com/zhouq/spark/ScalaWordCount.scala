package com.zhouq.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * scala 版本实现 wc
  *
  */
object ScalaWordCount {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")

    //创建spark 配置,设置应用程序名字
//    val conf = new SparkConf().setAppName("scalaWordCount")
    val conf = new SparkConf().setAppName("scalaWordCount").setMaster("local[4]")

//    conf.set("spark.testing.memory","102457600")

    //创建spark 执行的入口
    val sc = new SparkContext(conf)

    //制定以后从哪里读取数据创建RDD (弹性分布式数据集)

    //取到一行数据
    val lines: RDD[String] = sc.textFile(args(0))

    //切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))


    //按单词和一组合
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

    //按key 进行聚合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)

    // 排序
    val sorted = reduced.sortBy(_._2, false)

    //将结果保存到hdfs中
    sorted.saveAsTextFile(args(1))

    //释放资源
    sc.stop()

  }

}
