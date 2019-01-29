package com.zhouq.scala.day04.implicit_

import java.io.{BufferedReader, File, FileReader}

import com.zhouq.scala.day04.MyImplicit._

import scala.io.Source

/**
  * Create by zhouq on 2019/1/26
  *
  */

class RichFile(file: File) {

  /**
    * 返回文件的行数
    *
    * @return
    */
  def count(): Int = {
    val fileReader = new FileReader(file)
    val bufferedReader = new BufferedReader(fileReader)

    var sum = 0
    try {
      var line = bufferedReader.readLine()
      while (line != null) {
        sum += 1
        line = bufferedReader.readLine()
      }
    } catch {
      case _: Exception => sum
    } finally {
      if (fileReader != null) {
        fileReader.close()
      }

      if (bufferedReader != null) {
        bufferedReader.close()
      }
    }

    sum
  }
}

object ImplicitDemo {

  /**
    * 隐式转换
    *
    *     隐式参数
    *
    *     隐式转换类型
    *
    *     隐式类
    */

  implicit val x = 10

  def say(implicit content: String = "明天") = println(content)

  def add(a: Int)(implicit b: Int) = a + b

  /**
    * 方法的参数如果有多个隐式参数的话,只需要使用一个 implicit 关键字即可
    * 隐式参数列表必须放在方法的参数列表最后
    */
  def addPlus(a: Int)(implicit b: Int, c: Int) = a + b + c

  /**
    * 定义一个隐式转换的方法
    */
  implicit def double2Int(a: Double) = {
    println("doubleToInt")
    a.toInt
  }

  /**
    * 定义一个隐式类,隐式类智能定义在 单例的object 中
    */
  implicit val Fdouble2Int = (double: Double) => {
    println("Fdouble2Int")
    double.toInt
  }


  /**
    * 定义一个隐式类,隐式类智能定义在 单例的object 中
    */
  implicit class FileRead(file: File) {
    def read = Source.fromFile(file).mkString
  }

  def main(args: Array[String]): Unit = {

    say("中午好呀")

    /**
      * say 方法的参数是隐式的,如果没有传递参数的话,
      * 编译器在编译的时候会自动从上下文中去寻找一个隐式的值(符合参数类型的值)
      *
      */
    implicit val msg = "该吃中午饭了"
    // 编译器在查找隐式值的时候不能有歧义
    //    implicit val xx = "还能行?"  //不行滴

    say

    println(add(5)(5)) //10
    println(add(5)) // 15

    println("====================隐式转换类型======================")

    //age 是一个Int 类型,但是赋值的时候定义成一个double 类型,编译器会从上下文中去找是否存在把double 转换成Int的隐式转换
    val age: Int = 20.5

    println(age)

    val file = new File("G:\\stady\\scalatest\\b.log")
    println("count = " + file.count())

    println("====================调用隐式转化类读取文件内容======================")
    println(file.read)

  }

}
