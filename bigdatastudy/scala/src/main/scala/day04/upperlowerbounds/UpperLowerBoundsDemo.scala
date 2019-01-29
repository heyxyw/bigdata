package com.zhouq.scala.day04.upperlowerbounds

/**
  * Create by zhouq on 2019/1/26
  *
  * 上界
  * 视图界定
  * 下界
  */

/**
  * <: 上界  upper bounds
  * 类型Java 中的 <T extends Comparable>
  * 不会发生隐式转换,除非 手动制定泛型
  * T 实现了 Comparable 接口
  *
  */
//class CmpComm[T <: Comparable[T]](o1: T, o2: T) {
//  def bigger = if (o1.compareTo(o2) > 0) o1 else o2
//}


/**
  * 第一版 视图界定
  *
  * <% 视图界定 view bounds
  * 会发生隐式转换
  */

//class CmpComm[T <% Comparable[T]](o1: T, o2: T) {
//  def bigger = if (o1.compareTo(o2) > 0) o1 else o2
//}

/**
  * 第二版 视图界定
  *
  * <% 视图界定 view bounds
  * 会发生隐式转换
  */
class CmpComm[T <% Ordered[T]](o1: T, o2: T) {
  def bigger = if (o1 > o2) o1 else o2
}


class Student(val name: String, val age: Int) extends Ordered[Student] {
  override def compare(that: Student): Int = this.age - that.age
  override def toString = s"Student($name, $age)"
}


object UpperLowerBoundsDemo {

  def main(args: Array[String]): Unit = {

    //    val cmpomm = new CmpComm(Integer.valueOf(1), Integer.valueOf(2))

    val cmpomm = new CmpComm(2, 3) //在上界的时候会报错,使用视图界定不会
    //这里有隐式转换
    //    val cmpomm = new CmpComm[Integer](2,3)
    println(cmpomm.bigger)


    val zhouq = new Student("zhouq", 20)
    val heyxyw = new Student("heyxyw", 18)

    val cmp = new CmpComm[Student](zhouq, heyxyw)
    println(cmp.bigger)
  }
}
