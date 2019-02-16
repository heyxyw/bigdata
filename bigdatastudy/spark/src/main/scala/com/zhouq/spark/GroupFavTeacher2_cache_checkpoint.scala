package com.zhouq.spark

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求每门课程最受欢迎老师TopN  --2
  *   -- 使用cache
  *   -- 使用checkpoint 一般设置hdfs 目录
  *
  *
  */
object GroupFavTeacher2_cache_checkpoint {
  def main(args: Array[String]): Unit = {


    //前 N
    val topN = args(1).toInt

    //学科集合
    val subjects = Array("bigdata", "javaee", "php")

    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")

    //创建spark 执行入口
    val sc = new SparkContext(conf)

    //checkpoint 得先设置 sc 的checkpoint 的dir
//    sc.setCheckpointDir("hdfs://hdfs://hadoop1:8020/user/root/ck20190215")

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

    //第一种使用cache RDD 把数据缓存在内存中.标记为cache 的RDD 以后被反复使用,才使用cache
    val cached: RDD[((String, String), Int)] = reduced.cache()

    //第二种 使用checkpoint,得先设置 sc 的 checkpointDir
//    cached.checkpoint()

    /**
      * 先对学科进行过滤,然后再进行排序,调用RDD 的sortBy进行排序,避免scala 的排序当数据量大时,内存不足的情况.
      * take 是Action 操作,每次take 都会进行一次任务提交,具体查看日志打印情况
      */
    for (sub <- subjects) {
      //过滤出当前的学科
      val filtered: RDD[((String, String), Int)] = cached.filter(_._1._1 == sub)

      //使用RDD 的 sortBy ,内存+磁盘排序,避免scala 中的排序因内存不足导致异常情况.
      //take 是Action 的,所以每次循环都会触发一次提交任务,祥见日志打印情况
      val favTeacher: Array[((String, String), Int)] = filtered.sortBy(_._2, false).take(topN)

      println(favTeacher.toBuffer)

    }

    /**
      * 前面cache的数据已经计算完了，后面还有很多其他的指标要计算
      * 后面计算的指标也要触发很多次Action，最好将数据缓存到内存
      * 原来的数据占用着内存，把原来的数据释放掉，才能缓存新的数据
      */

    //把原来缓存的数据释放掉
    cached.unpersist(true)

    sc.stop()
  }
}
