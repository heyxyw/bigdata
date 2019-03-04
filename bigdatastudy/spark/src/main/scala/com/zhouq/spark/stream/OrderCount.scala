package com.zhouq.spark.stream

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Create by zhouq on 2019/2/24
  *
  */
object OrderCount {
  def main(args: Array[String]): Unit = {

    //创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("OrderCount").setMaster("local[*]")
    //创建SparkStreaming,并设置间隔时间
    val ssc = new StreamingContext(conf, Duration(5000))

    //指定 组名
    val group = "g1"
    //指定消费者的topic
    val topic = "orders"

    //准备kafka broker 地址
    val brokerList = "hadoop3:9092,hadoop4:9092,hadoop5:9092"
    //准备zk 地址,以后可以使用 mysql ,redis 来记录偏移量
    val zkQuorum = "hadoop1:2181,hadoop2:2181,hadoop3:2181"

    //创建stream 时使用的topic 集合,SparkStreaming可同时消费多个topic
    val topics = Set(topic)

    //创建一个 ZKGroupTopicDirs 对象,就是指定zk 写入偏移量的目录
    val topicDirs = new ZKGroupTopicDirs(group, topic)

    //获取zk 中的路径
    val zkTopicPath: String = s"${topicDirs.consumerOffsetDir}"


    //准备kafka 连接参数
    val kafkaParams = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> group,
      //从头开始读取数据
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )

    //创建zk 客户端
    val zkClient = new ZkClient(zkQuorum)

    //查询消费路径下是否有子节点 (默认有子节点为我们自己保存不同 partition 时生成的)
    // /g001/offsets/wordcount/0/10001"
    // /g001/offsets/wordcount/1/30001"
    // /g001/offsets/wordcount/2/10001"
    //zkTopicPath  -> /g001/offsets/wordcount/

    val children: Int = zkClient.countChildren(zkTopicPath)

    //保存kafka 起始消费位置,如果zk 中存在 offset 的话
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    //用于存储Stream 数据
    var kafkaStream: InputDStream[(String, String)] = null

    if (children > 0) {
      for (i <- 0 until children) {
        // /g001/offsets/wordcount/0/10001

        // /g001/offsets/wordcount/0  -> 10001
        val partitionOffset: String = zkClient.readData[String](s"$zkTopicPath/${i}")

        val tp = TopicAndPartition(topic, i)
        // /wordcount/0 -> 10001
        fromOffsets += (tp -> partitionOffset.toLong)
      }

      //Key: kafka的key   values: "hello tom hello bob"
      //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (kafka的key, message) 这样的 tuple

      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())

      //通过kafkaUtils 创建直连的 DStream (fromOffsets 参数作为起始偏移量继续消费kafka 数据)

      //[String, String, StringDecoder, StringDecoder,     (String, String)]
      //  key    value    key的解码方式  value的解码方式 ,自定义读取函数参数
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

    } else {
      //如果未保存,根据 kafkaStream 的配置使用最新(largest) 或者最旧(Smallest)的 offset
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }

    //记录偏移量的范围
    var offsetRanges = Array[OffsetRange]()

    kafkaStream.foreachRDD(kafkaRdd => {

      val offsetRanges: Array[OffsetRange] = kafkaRdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val lines: RDD[String] = kafkaRdd.map(_._2)
      lines.foreachPartition(it => {
        it.foreach(line => {
          println(line)
        })
      })

      //偏移量更新

      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/" + s"${o.partition}"
        println(zkPath)
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
      }

    })


    ssc.start()
    ssc.awaitTermination()
  }

}
