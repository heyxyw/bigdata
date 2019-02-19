package com.zhouq.spark.sql._1x

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkSQL 1.x demo
  *
  *
  */
object SQLDemo1 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SQLDemo").setMaster("local[4]")

    val sc = new SparkContext(conf)

    //SparkContext 不能创建特殊的RDD(DataFrame)
    // 定义一个 SQLContext,将 SparkContext 进行增强
    val sqlContext = new SQLContext(sc)

    //现有一个普通的RDD ,然后再关联上 scama 进而转换成 DataFrame

    val lines: RDD[String] = sc.textFile("H:\\bigdatatest\\spark\\persion")

    //将数据进行整理
    val boyRDD: RDD[Boy] = lines.map(line => {
      val fileds: Array[String] = line.split(",")
      val id: Long = fileds(0).toLong
      val name: String = fileds(1)
      val age: Int = fileds(2).toInt
      val fv: Int = fileds(3).toInt
      Boy(id, name, age, fv)
    })

    //该RDD 里面装的是Boy 类型,有了scama 信心,但是还是一个普通的RDD
    //将RDD 转化为 DataFrame
    //导入隐式转换
    import sqlContext.implicits._
    val bdf: DataFrame = boyRDD.toDF()

    //变成DF 后就可以使用两种API 进行编程了
    // 把DataFrame 先进行注册临时表
    bdf.registerTempTable("t_boy")

    //写 sql
    val result: DataFrame = sqlContext.sql("select * from t_boy order by fv desc,age asc ")

    result.show()

    sc.stop()

  }

}

case class Boy(id: Long, name: String, age: Int, fv: Int)
