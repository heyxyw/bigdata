package com.zhouq.spark.ip

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计访问日志ip 归属地
  *
  */
object IPSearch {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("IPSearch").setMaster("local[4]")

    val sc = new SparkContext(conf)

    //读取hdfs 规则文件
    val rulesLines: RDD[String] = sc.textFile(args(0))
    //整理ip规则数据

    val ipRulesRDD: RDD[(Long, Long, String)] = rulesLines.map(line => {
      val fileds: Array[String] = line.split("[|]")
      val startNum: Long = fileds(2).toLong
      val endNum: Long = fileds(3).toLong
      val province: String = fileds(6)
      (startNum, endNum, province)
    })

    //将分散到多个Executor 中的数据收集到Driver 中

    val rules: Array[(Long, Long, String)] = ipRulesRDD.collect()

    //将rules 广播到 Executor 中
    // 广播后会在Driver 端产生一个 引用 共后续使用
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rules)

    //读取日志数据
    val accessLines: RDD[String] = sc.textFile(args(1))

    //整理数据,提取出省份数据
    val proviceAndOne: RDD[(String, Int)] = accessLines.map(line => {
      //切分日志
      val fileds: Array[String] = line.split("[|]")
      //提取ip
      val ip: String = fileds(1)
      //计算ipNum
      val ipNum: Long = MyUtils.ip2Long(ip)

      //获取 rules 数据
      // 为什么这里需要再获取一次,不直接用上面的rules ,上面的rules 的数据是在Driver 上,
      // 但是这里的代码最终是在Executor 上面执行,Executor 上面有上面广播出去的规则数据,可以通过Driver 端的引用获取到这份数据.
      // Driver端广播变量的引用是怎样跑到Executor中的呢？ 在Driver 会通过Task 一起发送到Executor 上面去执行
      val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value

      //获取ip 的索引
      val index: Int = MyUtils.binarySearch(rulesInExecutor, ipNum)

      //查询ip 的省份地址
      var province = "未知"
      if (index != -1) {
        province = rulesInExecutor(index)._3
      }
      //发送省份数据
      (province, 1)
    })


    //聚合
    val reduced: RDD[(String, Int)] = proviceAndOne.reduceByKey(_ + _)

    // 这里不直接使用foreach 是因为,我们需要往数据库中插入数据,如果取一条数据插入一次,将会产生大量的数据库连接开销.
    // 所以,我们按照分区来进行循环读取插入,一个分区建立一次连接.
    reduced.foreachPartition(it => {

      //获取连接
      val connection: Connection = DriverManager.getConnection("jdbc:mysql://192.168.80.131:3306/bigdata?characterEncoding=UTF-8", "root", "root")
      val prst: PreparedStatement = connection.prepareStatement("insert into access_log(province,count) values (?,?) ")

      //填充数据
      it.foreach(tp => {
        prst.setString(1, tp._1)
        prst.setInt(2, tp._2)
        prst.executeUpdate()
      })

      //关闭资源
      if (prst != null) {
        prst.close()
      }

      if (connection != null) {
        connection.close()
      }

    })

    sc.stop()
  }


}
