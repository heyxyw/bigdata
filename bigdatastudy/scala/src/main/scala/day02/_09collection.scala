package com.zhouq.scala.day02

/**
  * Array:
  * 内容都可变
  * 长度可变数组(ArrayBuffer) 和长度不可变数组 Array
  *
  * scala 中 集合分为可变数组 mutable 和 不可变数据immutable
  *
  */
object _09collection {
  def main(args: Array[String]): Unit = {

    var list = List(1, 2, 3, 4)
    //表示拼接,生成一个字符串
    list.+("s") //String = List(1, 2, 3)ss

    list.foreach(x => println(x))
    // head 取头 1
    println(list.head)
    // tail 取尾 List(2, 3, 4)
    println(list.tail.toBuffer)

    // ++


    // ++:

    // :::

//    list.aggregate()

  }
}
