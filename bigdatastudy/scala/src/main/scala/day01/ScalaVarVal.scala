package com.zhouq.scala.day01

/**
  * //todo
  *
  *
  */

class Student(val name: String, var age: Int)

object ScalaVarVal {
  def main(args: Array[String]): Unit = {

    /**
      * 变量的定义可以是 var  和 val 修饰
      * var 修饰的变量是可以进行修改的
      * val 修饰的变量不可以进行改变，相当于Java 中的final 修饰的变量
      * var | val 变量名称： 类型 = 值
      *
      * Unit 数据类型相当于java 中的void ，是一对 （）
      *
      */

    val name = "zhouq"
    var age = 18
    println("name = " + name + ",age = " + age)

    // 动态传递
    println(f"姓名：$name%s 年龄：$age")


    val student = new Student("zhouq", 18)

    // 对象的取值

    println(s"${student.name},${student.age}")

    println(student.name)
    println(student.age)

    //表示占位符,参考java 的占位符,输出没有换行
    printf("%s 学费 %1.4f,网址是 %s",name,1234.345676,"asdafdads")

    val i: Int = 8

    /**
      * 条件表达式 if ..else if ... else
      *
      * val i = 8
      * val r = if(i >8) i // 编译器会默认认为else部分没有返回值，即Unit = ()
      * val r1: Any = if Any else Any
      *
      */

    //条件表达式使用
    //scala之后一行的值作为返回值
    var s = if (i > 10) {
      i
    } else {
      100
      0
    }
    println(s)

    //条件不满足 返回 ()
    var r = if (i < 8) i
    println(r)
  }

}
