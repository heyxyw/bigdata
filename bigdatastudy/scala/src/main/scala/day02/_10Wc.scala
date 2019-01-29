package com.zhouq.scala.day02

/**
  * //todo
  *
  */
object _10Wc {

  def main(args: Array[String]): Unit = {


    val lines = List("hello tom hello jerry", "hello hello zhouq hello tom")

    //第一种

    // 先进行分割+扁平化合并： List[String] = List(hello, tom, hello, jerry, hello, hello, zhouq, hello, tom)
    var r2 = lines.flatMap(_.split(" "))
      // 单词计数 1 ： List[(String, Int)] = List((hello,1), (tom,1), (hello,1), (jerry,1), (hello,1), (hello,1), (zhouq,1), (hello,1), (tom,1))
      .map((_, 1))
      // 按单词进行分组： Map(zhouq -> List((zhouq,1)), tom -> List((tom,1), (tom,1)), jerry -> List((jerry,1)), hello -> List((hello,1), (hello,1), (hello,1), (hello,1), (hello,1)))
      .groupBy(_._1)
      // 取value 进行计数： Map(zhouq -> 1, tom -> 2, jerry -> 1, hello -> 5)
      .mapValues(_.size)
      // map转 list : List[(String, Int)] = List((zhouq,1), (tom,2), (jerry,1), (hello,5))
      .toList
      // 按第二个元素倒序排序： List[(String, Int)] = List((hello,5), (tom,2), (zhouq,1), (jerry,1))
      .sortBy(x => -x._2)


    //第二种
    lines.flatMap(_.split(" ")).map((_,1)).groupBy(_._1)
      //foldLeft 函数去相加 避免 value 不都是 1 的情况
      // 第一个下划线 是表示上一次列表里面的数组
      // 第二个下划线 表示 foldLeft 表示上一次计算的结果，第一次计算就为初始值 0
      // 第三个下划线 表示 下一个需要累计的值，这里取元祖的第二个值
      // 最终完成累计
      .mapValues(_.foldLeft(0)(_ + _._2))
      .toList.sortBy(x => -x._2)

    println(r2.toBuffer)


    //第二种
    val arr1 = Array("hello zhouq memeda", "hello heyxyw love you")

    //Array[String] = Array(hello, zhouq, memeda, hello, heyxyw, love, you)
    val r3 = arr1.flatMap(_.split(" "))
      //Map(memeda -> Array(memeda), you -> Array(you), heyxyw -> Array(heyxyw), love -> Array(love), zhouq -> Array(zhouq), hello -> Array(hello, hello))
      .groupBy(x => x)
      //Map(memeda -> 1, you -> 1, heyxyw -> 1, love -> 1, zhouq -> 1, hello -> 2)
      .mapValues(x => x.length)
      // map 不能进行排序 所以先转换成 list
      //List((memeda,1), (you,1), (heyxyw,1), (love,1), (zhouq,1), (hello,2))
      .toList
      //List((hello,2), (memeda,1), (you,1), (heyxyw,1), (love,1), (zhouq,1))
      //  x => - x._2   '-' 表示降序,'+' 表示升序;  x._2 表示取第二个元素  ,整句话的意思就是按 每个元素里面的第二个元素进行升序排序
      .sortBy(x => -x._2)
    println(r3.toBuffer)


  }

}
