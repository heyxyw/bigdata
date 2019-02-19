package com.zhouq.spark.sql.join

import com.zhouq.spark.ip.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 使用 SparkSQL 方式计算 ip归属地
  * 在第一版中,使用join 的方式代价太昂贵
  * 当前我们的规则文件比较小,我们可以缓存起来进行广播.
  * 自定义一个UDF 函数,把ipNum 转换成 省份 .在执行sql 时,调用自定义函数 即可
  *
  */
object IPSearchLocationSQL2 {


  def main(args: Array[String]): Unit = {
    //spark 入口
    val sparkSession: SparkSession = SparkSession.builder().appName("IPSearchLocationSQL").master("local[*]").getOrCreate()

    //读取规则文件
    import sparkSession.implicits._
    val ruleLines: Dataset[String] = sparkSession.read.textFile(args(0))

    //整理ip规则数据
    val rulesInDriver: Array[(Long, Long, String)] = ruleLines.map(line => {
      val fileds: Array[String] = line.split("[|]")
      val startNum: Long = fileds(2).toLong
      val endNum: Long = fileds(3).toLong
      val province: String = fileds(6)
      (startNum, endNum, province)
    }).collect()

    //广播(必须使用 Broadcast)
    //将广播变量的引用返回到Driver端
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sparkSession.sparkContext.broadcast(rulesInDriver)

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


    //注册日志表
    ipDataFrame.createTempView("v_log")

    //定义一个自定义函数 UDF,并注册
    //该函数的功能是（输入一个IP地址对应的十进制，返回一个省份名称）
    sparkSession.udf.register("ip2province",(ipNum:Long) =>{
      //查找ip规则,由于ip 规则在Driver端已经被广播了.
      val ipRulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      
      //根据ipNum 计算省份
      val index: Int = MyUtils.binarySearch(ipRulesInExecutor,ipNum)
      var province = "未知"
      if (index != -1){
        province = ipRulesInExecutor(index)._3
      }

      province
    })

    //执行sql
    val r: DataFrame = sparkSession.sql("select ip2province(ip_num) as province,count(*) counts from  v_log group by province order by counts desc")

    r.show()

    sparkSession.stop()

  }

}
