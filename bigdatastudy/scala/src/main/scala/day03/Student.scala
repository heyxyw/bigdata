package com.zhouq.scala.day03

/**
  *
  *  构造器
  *  gender:String  == private[this] gender:String
  *
  **/

class Student(val id: String, var name: String,gender:String,var age:Int = 18) {

}


object Student {
  def main(args: Array[String]): Unit = {
    val  p = new Student("123","zhouq","xx",20)

//    p.id = "321"  //val 修饰不可修改
    p.name = "zhouq1"
//    p.gender  //无法访问

    p.age = 20
  }
}