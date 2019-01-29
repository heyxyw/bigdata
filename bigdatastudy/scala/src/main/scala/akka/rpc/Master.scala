package com.zhouq.rpc

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.duration._

/**
  * Create by zhouq on 2019/1/25
  *
  */
class Master(host: String, port: Int) extends Actor {

  var idToWorker = new mutable.HashMap[String, WorkerInfo]()

  var workers = new mutable.HashSet[WorkerInfo]()

  //超时检查的间隔
  val CHECK_INTERVAL = 15000

  override def preStart(): Unit = {
    println("preStart invoked")

    //导入隐式转换
    import context.dispatcher //使用akka的定时器, 要导入这个包
    context.system.scheduler.schedule(0 millis, CHECK_INTERVAL millis, self, CheckTimeOutWorker)
  }

  //用于接收消息
  override def receive: Receive = {
    //接收worker 注册信息
    case RegisterWorker(id, memory, cores) => {
      //判断worder 是否存在
      if (!idToWorker.contains(id)) {
        val worker = new WorkerInfo(id, memory, cores)
        worker.lastHeartbeatTime = System.currentTimeMillis()
        idToWorker(id) = worker
        workers += worker
        //响应 master 信息
        sender ! RegisteredWorkers(s"akka.tcp://MasterSystem@$host:$port/user/Master")
        println(id + ":注册成功")

      }

      println("当前存活worker数:" + workers.size)
    }
    //接收心跳信息
    case Heartbeat(id) => {
      //如果当前worker id 存在则续约
      if (idToWorker.contains(id)) {
        val workerInfo = idToWorker(id)
        workerInfo.lastHeartbeatTime = System.currentTimeMillis()
      } else {
        //重新注册
        sender() ! ReRegister
      }
    }
    //定时检查存活信息,若列表中的worker 最后一次上报时间超时,则剔除
    case CheckTimeOutWorker => {
      val currentTime = System.currentTimeMillis()
      val removeWorkerInfoes = workers.filter(x => currentTime - x.lastHeartbeatTime > CHECK_INTERVAL)

      for (w <- removeWorkerInfoes) {
        workers.remove(w)
        idToWorker.remove(w.id)
        println("移除worker:" + w.id)
      }

      println("当前存活worker数:" + workers.size)
    }
  }
}

object Master {
  def main(args: Array[String]): Unit = {

    val host = args(0)
    val port = args(1).toInt
    //准备配置
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    //创建actor
    val masterSystem = ActorSystem("MasterSystem", config)

    //创建master
    val master = masterSystem.actorOf(Props(new Master(host, port)), "Master")

  }
}
