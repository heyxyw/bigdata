package com.zhouq

import com.alibaba.fastjson.JSON
import com.typesafe.config.{Config, ConfigFactory}
import com.zhouq.utils.{JedisUtils, Utils}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import scalikejdbc._
import scalikejdbc.config.DBs

/**
  * 移动充值项目 实例 从kafka 消费 实时运算指标
  *
  */
object RTmonitor {

  def main(args: Array[String]): Unit = {

    //加载配置文件
    val config: Config = ConfigFactory.load()

    //创建kafka 参数
    val kafkaParams = Map(
      "metadata.broker.list" -> config.getString("kafka.broker.list"),
      "group.id" -> config.getString("kafka.group.id"),
      "auto.offset.reset" -> "smallest"
    )

    //准备 topic 信息
    val topics: Set[String] = config.getString("kafka.topics").split(",").toSet

    //准备spark 入口
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("实时统计")

    //准备SparkStreaming
    val ssc = new StreamingContext(sparkConf, Seconds(2))


    //从kafka 消费数据,从数据库中获取当前消费到的消费偏移量
    //    val fromOffsets: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
    //加载配置信息
    // 查询偏移量表 cmcc_streaming_offset
    DBs.setup()
    val fromOffsets: Map[TopicAndPartition, Long] = DB.readOnly { implicit session =>
      sql"select * from cmcc_streaming_offset where groupid =?".bind(config.getString("kafka.group.id")).map(rs => {
        (TopicAndPartition(rs.string("topic"), rs.int("partitions")), rs.long("offset"))
      }).list().apply()
    }.toMap


    //第一次启动
    val stream: InputDStream[(String, String)] = if (fromOffsets.size == 0) {

      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    } else {
      //非第一次启动

      //拿到kakfa 集群
      val kafkaCluster = new KafkaCluster(kafkaParams)

      //获取到所有分区对应的偏移量
      val earliestLeaderOffsets: Either[Err, Map[TopicAndPartition, KafkaCluster.LeaderOffset]] = kafkaCluster.getEarliestLeaderOffsets(fromOffsets.keySet)

      //校验过后的偏移量,避免出现因kafka 数据已经超过有消息导致的错误.
      var checkedOffset = Map[TopicAndPartition, Long]()

      if (earliestLeaderOffsets.isRight) {
        val topicAndPartitionToOffset: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = earliestLeaderOffsets.right.get
        //开始对比偏移量
        checkedOffset = fromOffsets.map(owner => {
          //kafka集群当前 topic 分区偏移量
          val clusterEarliestOffset: Long = topicAndPartitionToOffset.get(owner._1).get.offset
          //说明偏移量未过期
          if (owner._2 >= clusterEarliestOffset) {
            owner
          } else {
            //过期,使用当前集群的偏移量
            (owner._1, clusterEarliestOffset)
          }
        })

      }

      //处理kafka 数据的函数
      val messageHandler: MessageAndMetadata[String, String] => (String, String) = (mm: MessageAndMetadata[String, String]) => (mm.key(), mm.message())

      //使用校验过后的偏移量  checkedOffset
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, checkedOffset, messageHandler)
    }


    //处理数据=======业务

    /**
      * receiver 接受数据是在Executor端 cache -- 如果使用的窗口函数的话，没必要进行cache, 默认就是cache， WAL ；
      * 如果采用的不是窗口函数操作的话，你可以cache, 数据会放做一个副本放到另外一台节点上做容错
      * direct 接受数据是在Driver端
      */
    stream.foreachRDD(rdd => {
      //获取当前rdd 数据的偏移量
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      //解析数据
      val baseData: RDD[(String, String, List[Double], String, String)] = rdd.map(t => JSON.parseObject(t._2))
        //过滤数据，只需要充值日志
        .filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
        .map(jsonObj => {
          // 充值结果， 0000: 表示充值成功
          val bussinessRst: String = jsonObj.getString("bussinessRst")
          // 充值金额，充值成功才计算金额，否则为0
          // 变量指定为 Double ，才会自动发生隐式转换。
          val chargefee: Double = if (bussinessRst.equals("0000")) jsonObj.getDouble("chargefee") else 0

          // 充值成功次数，成功为 1 失败 为 0
          val isSucc = if (bussinessRst.equals("0000")) 1 else 0
          //获取充值时间

          val startTime = jsonObj.getString("requestId")
          // 获取充值到账时间
          val receiveNotifyTime = jsonObj.getString("receiveNotifyTime")

          // 获取充值消耗时间
          val costime: Long = if (bussinessRst.equals("0000")) Utils.caculateRqt(startTime, receiveNotifyTime) else 0

          //充值省份代码
          val provinceCode: String = jsonObj.getString("provinceCode")

          // 返回(日期,小时,List[总笔数，成功笔数，金额，消耗时间],省份代码,分钟数)
          ("A-" + startTime.substring(0, 8), startTime.substring(0, 10), List[Double](1, isSucc, chargefee, costime.toDouble), provinceCode, startTime.substring(0, 12))
        })

      // 实时报表 -- 业务概况
      /**
        * 1)统计全网的充值订单量, 充值金额, 充值成功率及充值平均时长.
        */
      baseData.map(t => (t._1, t._3))
        //合并数据
        .reduceByKey((list1, list2) => {
        // 使用拉链操作
        (list1 zip list2).map(x => x._1 + x._2)
      }).foreachPartition(itr => {
        // 存储数据
        // 获取jedis Client
        val client: Jedis = JedisUtils.getJedisClient()
        itr.foreach(tp => {
          client.hincrBy(tp._1, "total", tp._2(0).toLong)
          client.hincrBy(tp._1, "succ", tp._2(1).toLong)
          client.hincrByFloat(tp._1, "money", tp._2(2))
          client.hincrBy(tp._1, "timer", tp._2(3).toLong)
          //key 保存7天
          client.expire(tp._1, 60 * 60 * 24 * 7)
        })
        client.close()
      })


      // 每个小时的数据分布情况统计 总数，成功数
      baseData.map(t => ("B-" + t._2, t._3))
        .reduceByKey((list1, list2) => {
          (list1 zip list2).map(x => x._1 + x._2)
        }).foreachPartition(itr => {
        // 获取jedis Client
        val client: Jedis = JedisUtils.getJedisClient()

        itr.foreach(tp => {
          // B-2019022816
          client.hincrBy(tp._1, "total", tp._2(0).toLong)
          client.hincrBy(tp._1, "succ", tp._2(1).toLong)
          //保存七天
          client.expire(tp._1, 60 * 60 * 24 * 7)
        })

        client.close()
      })

      // 每天每个省份充值成功金额
      // 小时+省份作为 第一个参数，充值数据作为第二个参数
      baseData.map(t => ((t._2, t._4), t._3))
        .reduceByKey((list1, list2) => {
          (list1 zip list2) map (x => x._1 + x._2)
        }).foreachPartition(itr => {
        val client: Jedis = JedisUtils.getJedisClient()

        itr.foreach(tp => {
          //
          client.hincrBy("P-" + tp._1._1.substring(0, 8), tp._1._2, tp._2(3).toLong)
          client.expire("P-" + tp._1._1.substring(0, 8), 60 * 60 * 24 * 2)
        })

        client.close()
      })

      // 每分钟的数据分布情况统计
      baseData.map(t => ("C-" + t._5, t._3))
        .reduceByKey((list1, list2) => {
          (list1 zip list2) map (x => x._1 + x._2)
        }).foreachPartition(itr => {
        val client: Jedis = JedisUtils.getJedisClient()

        itr.foreach(tp => {
          client.hincrBy(tp._1, "succ", tp._2(1).toLong)
          client.hincrByFloat(tp._1, "money", tp._2(2))
          client.expire(tp._1, 60 * 60 * 24 * 2)
        })
        client.close()
      })

      // 记录偏移量
      offsetRanges.foreach(osr => {
        println(s"${osr.topic} ${osr.partition} ${osr.fromOffset} ${osr.untilOffset}")
        DB.autoCommit { implicit session =>
          sql"REPLACE INTO cmcc_streaming_offset(topic, groupid, partitions, offset) VALUES(?,?,?,?)"
            .bind(osr.topic, config.getString("kafka.group.id"), osr.partition, osr.untilOffset).update().apply()
        }

      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
