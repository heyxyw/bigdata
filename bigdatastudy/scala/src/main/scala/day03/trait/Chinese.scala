package com.zhouq.scala.day03.`trait`

/**
  * Create by zhouq on 2019/1/24
  * 在Scala中扩展类的方式和Java一样都是使用extends关键字
  * scala 中既可以继承类 也可以继承 trait，有多个 trait 使用 with
  */
class Chinese extends Animal with Human  {
  //当接口有默认实现时，必须用 override
  override def run(): Unit = {
    println("chinese")
  }

  def run1(): Unit = {
    println("chinese run1")
  }

  //在子类中重写超类的抽象方法时，不需要使用override关键字，写了也可以
  def bangz(): Unit = {
     println("hangzi")
   }

}

object Chinese {
  def main(args: Array[String]): Unit = {
    val chinese = new Chinese
    println(chinese.run())
  }
}