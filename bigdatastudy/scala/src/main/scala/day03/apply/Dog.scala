package com.zhouq.scala.day03.apply

/**
  *
  */

class Dog {

  println("dog class")
}

object Dog {

  println("dog object")

  def apply(): Dog = {
    //    println("apply invoked")
    new Dog
  }

  def apply(name: String): Unit = {
    println(name)
  }

  def main(args: Array[String]): Unit = {
    val d = Dog("lila")

    println(d)
    println("=============================================")
    val d1 = Dog
    val d2 = Dog

    println(d1)
    println(d2)
    println("=============================================")
    val d3 = Dog()
    val d4 = Dog()

    println(d3)
    println(d4)

  }

}
