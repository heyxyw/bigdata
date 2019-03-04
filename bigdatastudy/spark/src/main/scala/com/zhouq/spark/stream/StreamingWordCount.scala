package com.zhouq.spark.stream

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkStreaming 示例,连接socket 数据源
  *
  */
object StreamingWordCount {

  def main(args: Array[String]): Unit = {

    //spark 入口

    val conf: SparkConf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //StreamingContext是对SparkContext的包装，包了一层就增加了实时的功能
    //第二个参数是小批次产生的时间间隔: 5s
    val ssc = new StreamingContext(sc,Milliseconds(5000))

    //有了StreamingContext，就可以创建SparkStreaming的抽象了DSteam
    //从一个socket端口中读取数据
    //使用 nc 命令,简单搭建一个socket server
    val lines : ReceiverInputDStream[String] = ssc.socketTextStream("192.168.80.131",8888)
    //切分单词
    val words: DStream[String] = lines.flatMap(_.split(" "))
    //单词跟1 组合
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    //聚合
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)

    //打印结果
    reduced.print()

    //启动sparkstreaming 程序
    ssc.start()
    //优雅的退出,不再接收任务,等待已经执行的任务执行完成后结束应用.
    ssc.awaitTermination()

  }

}
