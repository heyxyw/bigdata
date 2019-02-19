package com.zhouq.spark.sql.join

import com.zhouq.spark.ip.MyUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 使用 SparkSQL 方式计算 ip归属地  使用join 的方式
  *
  */
object IPSearchLocationSQL {


  def main(args: Array[String]): Unit = {
    //spark 入口
    val sparkSession: SparkSession = SparkSession.builder().appName("IPSearchLocationSQL").master("local[*]").getOrCreate()

    //读取规则文件
    import sparkSession.implicits._
    val ruleLines: Dataset[String] = sparkSession.read.textFile(args(0))
    //整理ip规则数据
    val rulesDataFrame: DataFrame = ruleLines.map(line => {
      val fileds: Array[String] = line.split("[|]")
      val startNum: Long = fileds(2).toLong
      val endNum: Long = fileds(3).toLong
      val province: String = fileds(6)
      (startNum, endNum, province)
    }).toDF("snum", "enum", "province")

    //读取访问日志
    val accessLines: Dataset[String] = sparkSession.read.textFile(args(1))

    //整理访问数据ip
    val ipDataFrame: DataFrame = accessLines.map(line => {
      //切分日志
      val fileds: Array[String] = line.split("[|]")
      //提取ip
      val ip: String = fileds(1)
      //计算ipNum
      val ipNum: Long = MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    //注册临时视图
    rulesDataFrame.createTempView("v_rules")
    ipDataFrame.createTempView("v_ips")

    val r: DataFrame = sparkSession.sql("select province,count(*) counts from v_ips join v_rules on (ip_num >=snum and ip_num <= enum) group by province order by counts desc")

    r.show()

    sparkSession.stop()

  }

}
