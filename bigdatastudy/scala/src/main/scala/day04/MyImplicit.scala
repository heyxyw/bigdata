package com.zhouq.scala.day04

import java.io.File

import com.zhouq.scala.day04.implicit_.RichFile
import com.zhouq.scala.day04.upperlowerbounds.{Student2, Student3}

/**
  * Create by zhouq on 2019/1/26
  *
  *
  */
object MyImplicit {

  /**
    * 将文件隐式转换成 RichFile 类
    *
    * @param file
    * @return
    */
  implicit def file2RichFile(file: File) = new RichFile(file)


  //隐式转换 将Student  转换成 Ordered
  implicit def students2Ordered(stu: Student2) = new Ordered[Student2] {
    override def compare(that: Student2): Int = stu.age - that.age
  }

  //一个隐式对象实例
  implicit val comparatorStu = new Ordering[Student3] {
    override def compare(x: Student3, y: Student3): Int = x.age - y.age
  }
}
