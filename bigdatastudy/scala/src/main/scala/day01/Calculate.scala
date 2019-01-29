package com.zhouq.scala.day01

/**
  * 函数的传递 把函数作为参数传递给方法
  *
  */
object Calculate {

  //add 方法拥有两个int 参数的 函数 ,返回值为 两个int 值的和
  def add(a: Int, b: Int) = {
    a + b
  }


  // add2 拥有3个参数,第一个函数是 一个函数,第二个,第三个是 int 类型的参数
  // 第一个参数:
  //        是拥有2 个Int 类型的参数,返回值为 Int 类型的函数
  def add2(f:(Int, Int) => Int, a: Int, b: Int) = {
    f(a, b)
  }

  def add3(a:Int => Int,b: Int)={
    a(b) + b
  }


  var fxx = (a:Int, b:Int) => a + b

  val f1 = (a:Int) => a * 10

  def main(args: Array[String]): Unit = {


    println(add2((a,b) => a * b,2,3))

    var i1 = add(fxx(1,2),3)

    println(i1)

    var i2 = add3(f1,3)

    println(i2)


    val arr =Array(1,2,3)

    val array = arr.map((x:Int) => x * 10)
    val array1 = arr.map(f1)
    val array2 = arr.map(f1(_))

    println(array.toBuffer)
    println(array1.toBuffer)
    println(array2.toBuffer)

  }


}
