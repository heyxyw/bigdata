package com.zhouq.spark.customsort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 自定义排序实现 --1  定义一个对象实现 Ordered
  *
  */
object CustomSort1 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("CustomSort1").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //排序规则 :按照颜值的降序,如果颜值相等,按照年龄的升序
    // 模拟数据
    val users = Array("zhouq 18 99", "heyxyw 16 9999", "赵四 28 98", "张大脚 28 99")

    //将数据转化成RDD
    val lines: RDD[String] = sc.parallelize(users)

    //切分数据进行处理
    val userRDD: RDD[User] = lines.map(line => {
      val fileds: Array[String] = line.split(" ")
      val name: String = fileds(0)
      val age: Int = fileds(1).toInt
      val faceValue: Int = fileds(2).toInt

      new User(name, age, faceValue)
    })

    // 将RDD 里面封装的User 对象进行排序
    val sorted: RDD[User] = userRDD.sortBy(u => u)

    val result: Array[User] = sorted.collect()

    println(result.toBuffer)

    sc.stop()
  }

}

//  定义一个 User 实现 Ordered 或者 Ordering.
//  记住需要序列化.因为排序规则代码需要发送到Executor 端执行,么有序列化,会抛错

class User(val name: String, val age: Int, val fv: Int) extends Ordered[User] with Serializable {
  override def compare(that: User): Int = {
    if (this.fv == that.fv) {
      this.age - that.age
    } else {
      -(this.fv - that.fv)
    }
  }

  override def toString: String = s"name: $name, age: $age, fv: $fv"
}
