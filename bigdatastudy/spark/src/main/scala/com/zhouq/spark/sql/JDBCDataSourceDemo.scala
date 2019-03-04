package com.zhouq.spark.sql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * SparkSQL 连接数据库
  *
  */
object JDBCDataSourceDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JdbcDataSource")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val userDF: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://192.168.80.131:3306/bigdata?characterEncoding=UTF-8",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "user",
        "user" -> "root",
        "password" -> "root")
    ).load()

    //打印scama 信息
//    userDF.printSchema()
    //打印数据,show 是action 的
//    userDF.show()


    //调用filter方法,传入函数进行过滤
//    val filtered: Dataset[Row] = userDF.filter(r => {
//      r.getAs[Int](2)
//      r.getAs[Int]("age") <= 20
//    })
//

    //使用lambda 表达式
//    val r: Dataset[Row] = userDF.filter($"age" <= 20)
    val r: Dataset[Row] = userDF.where($"age" <= 20)
//    r.show()
    val relult: DataFrame = r.select($"id",$"name",$"age" * 10 as "age")

    var props = new Properties()
    props.put("user","root")
    props.put("password","root")

    relult.write.mode("ignore").jdbc("jdbc:mysql://192.168.80.131:3306/bigdata?characterEncoding=UTF-8","user1",props)


    spark.stop()
  }

}
