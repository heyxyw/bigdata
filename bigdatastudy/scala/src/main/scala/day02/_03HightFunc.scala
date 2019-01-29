package com.zhouq.scala.day02

/**
  * 高阶函数: 将其他函数作为参数或其结果是函数的函数
  *
  */
object _03HightFunc extends App {

  def apply (f:Int => String,v:Int ) = f(v)

  def layout(x:Int) = "======" + x.toString + "=======";

  println(apply(layout,10))
}
