package com.zhouq.spark.customsort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 自定义排序实现 --4 利用元祖自带的排序
  *
  */
object CustomSort5 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("CustomSort1").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //排序规则 :按照颜值的降序,如果颜值相等,按照年龄的升序
    // 模拟数据
    val users = Array("zhouq 18 99", "heyxyw 16 9999", "赵四 28 98", "张大脚 28 99")

    //将数据转化成RDD
    val lines: RDD[String] = sc.parallelize(users)

    //切分数据进行处理
    val userRDD: RDD[(String, Int, Int)] = lines.map(line => {
      val fileds: Array[String] = line.split(" ")
      val name: String = fileds(0)
      val age: Int = fileds(1).toInt
      val faceValue: Int = fileds(2).toInt
      (name, age, faceValue)
    })

    //利用元祖自带的排序,元祖排序先比较第一个,再比较第二个
    val sorted: RDD[(String, Int, Int)] = userRDD.sortBy(u => (u._2, u._3))

    val result: Array[(String, Int, Int)] = sorted.collect()

    println(result.toBuffer)
    sc.stop()
  }

}

