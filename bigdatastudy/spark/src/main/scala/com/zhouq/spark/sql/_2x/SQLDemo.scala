package com.zhouq.spark.sql._2x

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * SparkSQL 示例 2.x
  *
  */
object SQLDemo {
  def main(args: Array[String]): Unit = {

    //spark2.0 sql 的编程API
    // 2.X 入口
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("SQLDemo2.x")
      .master("local[*]")
      .getOrCreate()

    val lines: RDD[String] = sparkSession.sparkContext.textFile("H:\\bigdatatest\\spark\\persion")

    //将数据进行整理
    val rowRDD: RDD[Row] = lines.map(line => {
      val fileds: Array[String] = line.split(",")
      val id: Long = fileds(0).toLong
      val name: String = fileds(1)
      val age: Int = fileds(2).toInt
      val fv: Double = fileds(3).toDouble
      Row(id, name, age, fv)
    })

    //结果类型,其实就是表头,用来描述 DataFrame
    val scahema = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)
    ))

    val df: DataFrame = sparkSession.createDataFrame(rowRDD,scahema)

    import sparkSession.implicits._
    //过滤颜值大于99 按年龄排序
    val df2: Dataset[Row] = df.where($"fv" > 99).orderBy("age")

    df2.show()

    sparkSession.stop()
  }

}
