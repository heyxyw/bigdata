package com.zhouq.scala.day02

/**
  * 默认参数
  *
  */
object _02DefaultParams {

  def add(a: Int = 6, b: Int = 3) = {
    a + b
  }

  def main(args: Array[String]): Unit = {

    //调用时不传参数,即使使用函数,都会调用默认值
    println(add())
    // 调用时,传入参数,会覆盖方法默认值
    println(add(1, 2))
    // 调用时 值覆盖第一个参数的默认值
    println(add(1))

    //调用时,制定覆盖哪一个参数的值
    println(add(b = 9))
    //调用时,参数可以互换
    println(add(b = 1, a = 2))
    // 覆盖时,参数的名称只能跟方法定义的一致
//    println(add(c = 1, a = 2))
  }

}
