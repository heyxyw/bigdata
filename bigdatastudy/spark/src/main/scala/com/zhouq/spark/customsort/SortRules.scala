package com.zhouq.spark.customsort

/**
  * 排序规则隐式转换类
  *
  */
object SortRules {

  implicit object OrderingMeizi extends Ordering[Meizi] {
    override def compare(x: Meizi, y: Meizi): Int = {
      if (x.fv == y.fv) {
        x.age - y.age
      } else {
        y.fv - x.fv
      }
    }
  }

}
