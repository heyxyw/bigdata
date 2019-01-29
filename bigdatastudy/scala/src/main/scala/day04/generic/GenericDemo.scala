package com.zhouq.scala.day04.generic

import com.zhouq.scala.day04.generic.ClothesEnum.ClothesEnum

/**
  * Create by zhouq on 2019/1/26
  * 泛型示例
  */

abstract class Message[T](content: T)

class StrMessage(context: String) extends Message(context)

class IntMessage(context: Int) extends Message(context)

//定义一个泛型类衣服
class Clothes[A, B, C](val clothType: A, val color: B, val size: C)


//定义一个枚举类
object ClothesEnum extends Enumeration {

  type ClothesEnum = Value

  val 衬衫, 内衣, 外套, 裤子 = Value
}

object GenericDemo {

  def main(args: Array[String]): Unit = {
    val clothes = new Clothes[ClothesEnum, String, Int](ClothesEnum.裤子, "black", 180)

    println(clothes.size)

    val clothes1 = new Clothes[ClothesEnum, String, String](ClothesEnum.外套, "black", "XXL")

    println(clothes1.size)

    val clothes2 = new Clothes[ClothesEnum, String, String](ClothesEnum.内衣, "black", "30A")

    println(clothes2.size)

  }

}
