package com.zhouq.scala.day02

/**
  * 偏函数
  *
  */
object _06ParialFunction {

  def func(str: String): Int = {
    if (str.equals("a")) 97
    else 0
  }

  // macth case
  def func1: PartialFunction[String, Int] = {
    case "a" => 97
    case _ => 0
  }

  def f1: PartialFunction[Any, Int] = {
    //对 int 类型进行匹配处理,穷他的类型值不处理
    case i: Int => i * 10
  }


  def main(args: Array[String]): Unit = {
    println(func("b"))
    println(func1("a"))

    //不符合条件的数据是不会处理的
    val arr = Array(1, 2, 3, "1234")

    val collect = arr.collect(f1)

    println(collect.toBuffer)

  }

}
