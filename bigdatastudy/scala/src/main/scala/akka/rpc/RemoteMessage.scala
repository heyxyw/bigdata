package com.zhouq.rpc

/**
  * Create by zhouq on 2019/1/25
  *
  */
trait RemoteMessage extends Serializable

//Woker -> master
case class RegisterWorker(id: String, memory: Int, cores: Int) extends RemoteMessage


//master -> worker
case class RegisteredWorkers(masterUrl: String) extends RemoteMessage

//重新注册
case object ReRegister

//worker ->self
case object SendHeartbeat

//心跳信息
case class Heartbeat(id: String) extends RemoteMessage

// Master -> self 检查存活的woker
case object CheckTimeOutWorker