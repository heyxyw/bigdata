package com.zhouq.scala.day02

/**
  * 柯里化函数---结合隐式转换
  *
  *
  */
object _05Curring extends App {

  def add(a: Int, b: Int) = a + b

  def add1(a: Int)(b: Int) = a + b

  def add2(a: Int) = (b: Int) => a + b

}
