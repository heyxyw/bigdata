package com.zhouq.scala.day01

/**
  * 循环  及 yield 关键字
  *
  *
  */
object For {

  def main(args: Array[String]): Unit = {

    var arr = Array(1,3,56,35,234,73,53)

    // 遍历
    for (ele <- arr){
      println(ele)
    }

    println("-------------------------------------------------")

    for (ele <- 0 to arr.length) println(ele)

    println("-------------------------------------------------")

    // 守卫
    for (i <- arr if i % 2 == 0) println(i)

    println("-------------------------------------------------")

    /**
      * 循环
      *     for(变量 <- 表达式/集合/数组; if 守卫)
      *     for(i <- 0 to 3; j <- 0 to 3 if i!=j)
      *     yield
      *     to  0 to 3 =>返回一个0到3的范围区间，左右都是闭区间，都包含边界值
      *     until 0 until 3 => 返回一个0到2的范围区间，左闭右开区间，包含左边边界值，不包含右边边界值
      */
    // 双层 for
    for ( i <- 1 to 3;j <- 1 to 3 if i != j) println(10 * i + j)


    // yield 关键字 把 数据筛选出来 重新放入新的数据中 .
    // 把偶数放出来放入新的数组
    var r1 = for (i <- arr if i % 2 ==0) yield i;
    println(r1.toBuffer)

  }



}
