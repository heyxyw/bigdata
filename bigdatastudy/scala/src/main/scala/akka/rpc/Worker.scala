package com.zhouq.rpc

import java.util.UUID

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

/**
  * Create by zhouq on 2019/1/25
  *
  */
class Worker(masterHost: String, masterPort: Int, val memory: Int, val cores: Int) extends Actor {

  var master: ActorSelection = _
  var workerId = UUID.randomUUID().toString
  //多久发送一次心跳信息
  val HEART_INTERVAL = 5000


  override def preStart(): Unit = {
    master = context.actorSelection(s"akka.tcp://MasterSystem@$masterHost:$masterPort/user/Master")
    //发送注册信息
    master ! RegisterWorker(workerId, memory, cores)

  }

  override def receive: Receive = {

    //接收master 响应信息
    case RegisteredWorkers(masterUrl) => {
      println("masterUrl:" + masterUrl)
      //启动一个定时器,向master 发送心跳
      //多长时间后执行 单位,多长时间执行一次 单位, 消息的接受者 ,先发送给自己,可以在里面做一些逻辑操作
      import context.dispatcher
      context.system.scheduler.schedule(0 millis, HEART_INTERVAL millis, self, SendHeartbeat)
    }

    //定时向发送心跳
    case SendHeartbeat => {
      println("send heartbeat to master")
      master ! Heartbeat(workerId)
    }

    //重新提交注册信息
    case ReRegister => {
      println(workerId + ":重新提交注册")
      master ! RegisterWorker(workerId, memory, cores)
    }
  }
}


object Worker {

  def main(args: Array[String]): Unit = {

    val host = args(0)
    val port = args(1).toInt

    val masterHost = args(2)
    val masterPort = args(3).toInt

    //准备配置
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    //创建actor
    val workerSystem = ActorSystem("WorkerSystem", config)

    //创建 worker 并发送消息给master
    val worker = workerSystem.actorOf(Props(new Worker(masterHost, masterPort, 20480, 4)), "Worker")

  }
}