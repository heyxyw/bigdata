package com.zhouq.spark.customsort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 自定义排序实现 --4 利用 隐式转换
  *
  */
object CustomSort6 {

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
    // Ordering[(Int, Int)]  最终比较规则的格式
    // on[(String, Int, Int)]  未比较前的数据格式
    // (t => (t._2, t._3))  怎样将规则转换成想要比较的格式
    //先颜值降序,再年龄升序
    implicit val rules = Ordering[(Int, Int)].on[(String, Int, Int)](t => (-t._3, t._2))
    val sorted: RDD[(String, Int, Int)] = userRDD.sortBy(u => u)

    val result: Array[(String, Int, Int)] = sorted.collect()

    println(result.toBuffer)
    sc.stop()
  }

}

