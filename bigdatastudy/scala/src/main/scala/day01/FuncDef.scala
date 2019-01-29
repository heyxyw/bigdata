package com.zhouq.scala.day01

/**
  * 方法的定义，使用关键字def
  *
  * def 方法名称（参数列表）：方法的返回值类型 = 方法体
  *
  * 方法可以转换成函数 方法名称 _
  *
  * 函数的定义：（=>）
  *     方式一：
  *         (函数的参数列表) => 函数体
  *     val add = (x: Int, y: Int) => x + y
  *
  *     方式二：
  *         (函数的参数类型列表)=> 函数的返回值类型 = (函数的参数变量引用) => 函数体
  *     val add:(Int, Int) => Int = (a, b) => a + b
  *     val prtf: String => Unit = msg => println(msg)
  *
  * var a: Int = 2 + 2
  * def add(f:(Int, Int) => Int, a: Int, b: Int) = {
  *     f(a, b)
  * }
  *
  * 传名调用&传值调用
  *
  * val f = (a: Int, b: Int) => a + b
  * val f1 = (a: Int, b: Int) => a - b
  * val f2 = (a: Int, b: Int) => a * b
  * add(f, 2 + 8, 6) {
  *     f(10, 6) // 10 + 6
  * }
  *
  *
  */
object FuncDef {

  def main(args: Array[String]): Unit = {
    println(sum(1, 2))

    sayHello
    sayHello1
    sayHello1()
  }

  def sum(a: Int, b: Int): Int = a + b

  def sayHello = println("hello")

  def sayHello1() = println("hello zhouq")




}
