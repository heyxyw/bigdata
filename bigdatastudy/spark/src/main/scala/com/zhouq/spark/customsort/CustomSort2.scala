package com.zhouq.spark.customsort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 自定义排序实现 --2 在处理数据时,可以不传入对象,直接封装元祖对象.
  *
  */
object CustomSort2 {

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

    // 将RDD 里面封装的元祖进行排序,排序是没用到 name ,可以不传入
    val sorted: RDD[(String, Int, Int)] = userRDD.sortBy(u => new Boy(u._2, u._3))

    val result: Array[(String, Int, Int)] = sorted.collect()

    println(result.toBuffer)

    sc.stop()
  }

}

//  定义一个实现 Ordered 或者 Ordering.
//  记住需要序列化.因为排序规则代码需要发送到Executor 端执行,么有序列化,会抛错

class Boy(val age: Int, val fv: Int) extends Ordered[Boy] with Serializable {
  override def compare(that: Boy): Int = {
    if (this.fv == that.fv) {
      this.age - that.age
    } else {
      -(this.fv - that.fv)
    }
  }
}
