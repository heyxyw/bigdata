package com.zhouq.scala.day03

import scala.io.Source

/**
  * //todo
  *
  */
class MissRight {

  //在new 的时候，主构造器的都用会执行
  println(123)

  //
  def sayHi:Unit = {
    println("HI ....")
  }

  //相当于Java 的局部代码块
  try {
    val cont: String = Source.fromFile("G:\\stady\\bigData\\传智播客\\资料\\day26\\HelloScala\\src\\main\\scala\\cn\\itcast\\thread\\Thread01.scala").mkString

    println(cont)
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    println("finally。。。。。。")
  }

}

object MissRight{
  def main(args: Array[String]): Unit = {
    val m = new MissRight
    m.sayHi
  }
}
