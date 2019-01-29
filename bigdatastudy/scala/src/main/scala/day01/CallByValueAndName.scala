package com.zhouq.scala.day01

/**
  * 对象的调用
  *
  *
  */
object CallByValueAndName {

  var money = 100;

  def huaq(cost: Int) = {
     money = 100 - cost
  }

  def shuqian() = {
    money
  }


  def main(args: Array[String]): Unit = {

    huaq(10)

    shuqian()

  }
}
