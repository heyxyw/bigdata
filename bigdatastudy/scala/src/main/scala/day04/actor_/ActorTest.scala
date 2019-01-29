package com.zhouq.scala.day04.actor_

import scala.actors.Actor

/**
  * Create by zhouq on 2019/1/24
  *
  */
object MyActor1 extends Actor {
  def act(): Unit = {
    for (i <- 1 to 10) {
      println("actor-1 " + i)
      Thread.sleep(2000)
    }
  }
}

object MyActor2 extends Actor {
  def act(): Unit = {
    for (i <- 1 to 10) {
      println("actor-2 " + i)
      Thread.sleep(2000)
    }
  }
}

object ActorTest {

  def main(args: Array[String]): Unit = {
    //启动Actor
    MyActor1.start()
    MyActor2.start()
  }
}
