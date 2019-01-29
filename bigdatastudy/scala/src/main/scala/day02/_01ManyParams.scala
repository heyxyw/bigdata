package com.zhouq.scala.day02

/**
  * 可变参数
  *
  */
object _01ManyParams {

  def add(ints: Int*):Int = {
    var  sum = 0;
    for (i <- ints){
      sum += i;
    }
    sum
  }

  /**
    * 定义任意类型
    * @param params
    */
  def makePersion(params: Any*):Unit = {

  }

  def main(args: Array[String]): Unit = {

    val sum = add(1,2,3,4)

    println(sum)

  }
}
