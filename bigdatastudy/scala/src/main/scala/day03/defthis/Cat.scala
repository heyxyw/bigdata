package com.zhouq.scala.day03.defthis

/**
  * Create by zhouq on 2019/1/24
  *
  * 用this关键字定义辅助构造器
  */
class Cat {

  var name :String = "xxx"

  //
  def this(name: String){
    //每个辅助构造器必须以主构造器或其他的辅助构造器的调用开始，
    // 第一行一定是主构造器
    this()
    println("执行辅助构造器")
    this.name = name
  }

}

object Cat {
  def main(args: Array[String]): Unit = {
    val  c = new Cat("cat")
    println(c.name)
  }
}
