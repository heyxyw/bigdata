package com.zhouq.scala.day02

import java.util.Date

/**
  *
  * 待定参数
  *
  */
object _04PartParamFunc extends App {

  def log(date: Date, message: String) = {
    println(s"${date},${message}")
  }

  val date = new Date()

  //调用log 的时候,传递一个具体的时间,message 作为一个待定的参数

  //logBrandDate 成为一个新的函数,只有log 部分的参数待定(message)
  val logBrandDate: String => Unit = log(date, _: String)

  //调用的时候只需要传入 待定参数message即可
  logBrandDate("wocao")

  log(date, "wocao")

}
