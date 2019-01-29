package com.zhouq.scala.day03

import scala.collection.mutable.ArrayBuffer

/**
  * scala 中 object 就是单例的
  * 在Scala中没有静态方法和静态字段，但是可以使用object这个语法结构来达到同样的目的
  */
object SingletonDemo {
  def main(args: Array[String]): Unit = {
    val session = SessionFactory.getSession()

    println(session)

  }
}

object SessionFactory{
  var counts = 5

  val sessions = new ArrayBuffer[Session]()

  for( e <- 0 to counts) sessions += new Session

  def getSession():Session = {
    sessions.remove(0)
  }
}

class Session {

}
