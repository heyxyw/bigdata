package com.zhouq.scala.day02

/**
  *
  */
object _08ArrayOpt {

  def main(args: Array[String]): Unit = {

    val arr = Array(1, 2, 3, 6, 7)

    val fx = (x: Int) => x * 10

    // map映射
    //map 通过映射以后,会返回一个新的数组
    val r11 = arr.map(fx)

    println(r11.toBuffer)


    //各种简写变种

    arr.map((x: Int) => x * 10)

    arr.map(x => x * 10)

    arr.map(_ * 10)

    // faltten 扁平化操作

    val arr1 = Array("hello zhouq memeda", "hello heyxyw love you")

    val r0 = arr1.map(_.split(" "))

    println(r0.toBuffer)

    //先切割,再合并
    val r1 = arr1.map(_.split(" ")).flatten

    println(r1.toBuffer)

    //跟上面效果一样
    val r2 = arr1.flatMap(_.split(" "))

    println(r2.toBuffer)


    //foreach

    arr1.flatMap(_.split(" ")).foreach(println)


    arr1.reduce(_ + _)


  }
}
