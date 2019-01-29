package com.zhouq.scala.day03

/**
  * //todo
  *  object 类名称 跟 类名称相同
  * protected 之类能访问 [zhouq] 包名下能访问  private 构造器私有的，只有伴生对象下才能访问
  */
 protected[zhouq] class Persion private{
  val id = 123  //getId
  var name = "zhouq"  //getName  setName
  //private 修饰的只有在伴生对象中才能访问
  private var gender = "male"

  //this 修饰的只能在当前类访问，伴生对象也不能访问
  private[this] var pop:String = _

  def printPop:Unit = {
    println(pop)
  }
}


//伴生对象 跟 class 同名
object Persion {
  def main(args: Array[String]): Unit = {
    val p = new Persion
    println(p.id + "===" + p.name)
    //val 修饰的变量再进行赋值 ，会报错
//    p.id = 123

    p.gender = "xx"

//    p.pop  //this 修饰的只能在当前类访问

    println(p.printPop)

  }
}
