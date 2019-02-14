package com.zhouq.spark

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 求每门课程最受欢迎老师TopN  --3 自定义分区器
  *
  */
object GroupFavTeacher3 {
  def main(args: Array[String]): Unit = {

    //前 N
    val topN = args(1).toInt

    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")

    //创建spark 执行入口
    val sc = new SparkContext(conf)

    //指定读取数据
    val lines: RDD[String] = sc.textFile(args(0))

    val subjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      var teacher = line.substring(index + 1)
      var httpHost = line.substring(0, index)
      var subject = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })

    //将学科,老师联合当做key
    val reduced: RDD[((String, String), Int)] = subjectTeacherAndOne.reduceByKey(_ + _)

    //计算有多少学科
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()

    //进行分区
    //partitionBy 按照指定的规则进行分区
    //调用partitionBy 传入的数据格式是 (String,String)
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(new SubjectParitioner(subjects))

    //mapPartitions: 如果一次拿出一个分区(可以操作一个分区中的数据)
    val sorted: RDD[((String, String), Int)] = partitioned.mapPartitions(it => {
      //将迭代器转换成List ,突然进行排序,再转换成迭代器返回
      it.toList.sortBy(_._2).reverse.take(topN).iterator
    })

    //收集结果
    val favTeacher: Array[((String, String), Int)] = sorted.collect()

    println(favTeacher.toBuffer)

    sc.stop()
  }
}

class SubjectParitioner(sbs: Array[String]) extends Partitioner {

  //new 的时候会执行一次
  //用于存放规则的map
  var rule = new mutable.HashMap[String, Int]()
  //分区编号
  var i = 0
  for (sb <- sbs) {
    rule(sb) = i
    i += 1
  }

  //返回分区的数量(下一个RDD 有多少分区)
  override def numPartitions: Int = sbs.length

  //根据传入的key 计算分区标号
  //key 是一个元祖,格式 (String,String)
  override def getPartition(key: Any): Int = {
    //读取学科
    val subject: String = key.asInstanceOf[(String, String)]._1
    //根据分区规则返回分区编号
    rule(subject)
  }
}
