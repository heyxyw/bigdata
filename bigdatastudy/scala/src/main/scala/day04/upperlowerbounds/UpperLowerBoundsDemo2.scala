package com.zhouq.scala.day04.upperlowerbounds

/**
  * Create by zhouq on 2019/1/26
  *
  * 上界
  * 视图界定
  * 下界
  */

/**
  * 第二版 视图界定
  *
  * <% 视图界定 view bounds
  * 会发生隐式转换
  */
class CmpComm2[T <% Ordered[T]](o1: T, o2: T) {
  def bigger = if (o1 > o2) o1 else o2
}

//注意跟第一版的对比 .这里没有实现 Ordered 接口,就需要做一个隐式转换的方法
class Student2(val name: String, val age: Int) {
  override def toString = s"Student2($name, $age)"
}


object UpperLowerBoundsDemo2 {

  def main(args: Array[String]): Unit = {

    val zhouq = new Student2("zhouq", 20)
    val heyxyw = new Student2("heyxyw", 18)

    import com.zhouq.scala.day04.MyImplicit._
    val cmp = new CmpComm2[Student2](zhouq, heyxyw)
    println(cmp.bigger)
  }
}
