package com.zhouq.spark.jdbc

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * jdbc rdd 代码示例
  *
  */
object JBDCRddDemo {

  val getCoon = () => {
    DriverManager.getConnection("jdbc:mysql://192.168.80.131:3306/bigdata?characterEncoding=UTF-8", "root", "root")
  }

  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("JBDCRddDemo").setMaster("local[4]")

    var sc = new SparkContext(conf)

    val sql = "select * from access_log where id >= ? and id < ?"

    //分区参数可以设置为多个,执行的查询逻辑都是一致的
    val jdbcRdd = new JdbcRDD[(Int, String, Int)](
      sc,
      getCoon,
      sql,
      1,
      5,
      2, // 表示两个分区
      rst => {
        val id: Int = rst.getInt(1)
        val name: String = rst.getString(2)
        val age: Int = rst.getInt(3)
        (id, name, age)
      }
    )

    //循环打印
    jdbcRdd.foreachPartition(it => {
      println(it.toList.toBuffer)
    })

    sc.stop()
  }

}
