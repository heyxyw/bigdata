package com.zhouq.rpc

/**
  * Create by zhouq on 2019/1/25
  * worker 信息
  */
class WorkerInfo(val id: String,val memory: Int, val cores: Int) {

  //worker 的最后一次心跳时间
  var lastHeartbeatTime: Long = _
}
