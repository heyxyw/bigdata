package com.zhouq.spark.stream

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkStreaming 连接kafka 计算单词
  *
  *
  */
object KafkaWordCount {
  def main(args: Array[String]): Unit = {

    //spark 入口
    val conf: SparkConf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))

    val zkQuorum = "hadoop1:2181,hadoop2:2181,hadoop3:2181"
    val groupId = "g1"
    val topic = Map[String, Int]("kafkaWordCount1" -> 1)

    val kafkaDs: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic)

    val lines: DStream[String] = kafkaDs.map(_._2)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))

    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)

    //打印结果(Action)
    reduced.print()
    //启动sparksteaming程序
    ssc.start()
    //等待优雅的退出
    ssc.awaitTermination()

  }

}
