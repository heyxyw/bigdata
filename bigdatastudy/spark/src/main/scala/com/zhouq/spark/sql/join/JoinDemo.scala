package com.zhouq.spark.sql.join

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * SparkSQL Join 测试
  *
  */
object JoinDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("JoinDemo")
      .master("local[*]")
      .getOrCreate()


    import sparkSession.implicits._
    val userLines: Dataset[String] = sparkSession.createDataset(List("1,zhouq,cn", "2,heyxyw,us", "3,lzll,jp"))

    //整理数据
    val users: Dataset[(Long, String, String)] = userLines.map(line => {
      val fileds: Array[String] = line.split(",")
      val id: Long = fileds(0).toLong
      val name: String = fileds(1)
      val nation: String = fileds(2)
      (id, name, nation)
    })

    //转化为DF
    val df1: DataFrame = users.toDF("id", "name", "nation")

    val nationLines: Dataset[String] = sparkSession.createDataset(List("cn,中国", "us,美国"))

    val nations: Dataset[(String, String)] = nationLines.map(line => {
      val fileds: Array[String] = line.split(",")
      val ename: String = fileds(0)
      val cname: String = fileds(1)
      (cname, ename)
    })

    val df2: DataFrame = nations.toDF("cname", "ename")
    //
    //    // 第一种: 注册视图
    //
    //    df1.createTempView("v_user")
    //    df2.createTempView("v_nation")
    //
    //    val result: DataFrame = sparkSession.sql("select name,cname,ename from v_user join v_nation on nation = ename")
    //
    //    result.show()

    //第二种:使用 DataFrame 的 join 不需要引入隐式转换
    val result: DataFrame = df1.join(df2, $"nation" === $"ename", "left")

    result.show()
    sparkSession.stop()
  }
}
