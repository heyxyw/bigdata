package com.zhouq.scala.day04.upperlowerbounds

/**
  * Create by zhouq on 2019/1/26
  *
  * 上界
  * 视图界定
  * 下界
  */

/**
  * 第三版 上下文界定
  *
  */
class CmpComm3[T: Ordering](o1: T, o2: T)(implicit cmptor: Ordering[T]) {
  def bigger = if (cmptor.compare(o1, o2) > 0) o1 else o2
}

//在内部定义方法
class CmpComm3_1[T: Ordering](o1: T, o2: T) {
  def bigger = {
    def inner(implicit cmptor: Ordering[T]) = cmptor.compare(o1, o2)
    if (inner > 0) o1 else o2
  }
}

// 使用implicitly 关键字
class CmpComm3_2[T: Ordering](o1: T, o2: T) {
  def bigger = {
    val cmptor = implicitly[Ordering[T]]
    if (cmptor.compare(o1, o2) > 0) o1 else o2
  }
}

//注意跟第一版的对比 .这里没有实现 Ordered 接口,就需要做一个隐式转换的方法
class Student3(val name: String, val age: Int) {
  override def toString = s"Student2($name, $age)"
}


object UpperLowerBoundsDemo3 {

  def main(args: Array[String]): Unit = {

    val zhouq = new Student3("zhouq", 20)
    val heyxyw = new Student3("heyxyw", 18)

    import com.zhouq.scala.day04.MyImplicit._

    val cmp0 = new CmpComm3[Student3](zhouq, heyxyw)
    val cmp1 = new CmpComm3_1[Student3](zhouq, heyxyw)
    val cmp2 = new CmpComm3_2[Student3](zhouq, heyxyw)
    println(cmp0.bigger)
    println(cmp1.bigger)
    println(cmp2.bigger)
  }
}
