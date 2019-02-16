package com.zhouq.spark.ip

import scala.io.{BufferedSource, Source}

/**
  * Create by zhouq on 2019/2/16
  *
  */
object IPSearchLocation {

  /**
    * 将ip转化成十进制
    *
    * @param ip ip地址
    * @return
    */
  def ip2Long(ip: String): Long = {
    val fargments: Array[String] = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fargments.length) {
      ipNum = fargments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /**
    * 加载规则文件
    *
    * @param path 文件路径
    * @return (开始地址十进制,结束地址十进制,省份)
    */
  def readRules(path: String): Array[(Long, Long, String)] = {
    //加载ip规则文件
    val bs: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bs.getLines()

    //对IP进行解析,加载进内存
    val array: Array[(Long, Long, String)] = lines.map(line => {
      val fileds: Array[String] = line.split("[|]")
      val startNum: Long = fileds(2).toLong
      val endNum: Long = fileds(3).toLong
      val province: String = fileds(6)
      (startNum, endNum, province)
    }).toArray

    array
  }

  /**
    * 使用二分法查找索引
    *
    * @param lines 规则集合
    * @param ip    ip
    */
  def binarySearch(lines: Array[(Long, Long, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {
    //加载ip规则文件
    val rules: Array[(Long, Long, String)] = readRules("H:\\bigdatatest\\spark\\ip.txt")
    //计算ip nun
    val ipNum: Long = ip2Long("110.185.16.193")
    // 查找ipNum 对应的索引
    val index: Int = binarySearch(rules, ipNum)

    println(rules(index)._3)
  }


}
