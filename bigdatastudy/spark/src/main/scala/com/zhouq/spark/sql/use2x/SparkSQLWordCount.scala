package com.zhouq.spark.sql.use2x

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * spark sql wordCount
  *
  */
object SparkSQLWordCount {
  def main(args: Array[String]): Unit = {

    //spark2.0 sql 的编程API
    // 2.X 入口
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("SparkSQLWordCount")
      .master("local[*]")
      .getOrCreate()

    // Dataset 分布式数据集,是对RDD 的进一步封装
    // Dataset 只有一列,默认这列叫 value
    val lines: Dataset[String] = sparkSession.read.textFile("H:\\bigdatatest\\spark\\wd")

    //整理数据(切分压平)
    //导入隐式转换
    import sparkSession.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    // 注册视图
//
//    words.createTempView("v_wc")
//
//    val result: DataFrame = sparkSession.sql("select value as word,count(*) as counts from v_wc group by value order by counts desc")

    //Dataset DSL
//    val result: Dataset[Row] = words.groupBy($"value" as "word").count().sort($"count" desc)


    import org.apache.spark.sql.functions._
    val result: Dataset[Row] = words.groupBy($"value" as "word").agg(count("*") as "counts").orderBy($"counts" desc)

    result.show()

    sparkSession.stop()
  }


}
