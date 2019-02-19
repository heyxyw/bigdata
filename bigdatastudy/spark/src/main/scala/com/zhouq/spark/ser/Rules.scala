package com.zhouq.spark.ser

/**
  * 规则类,结合SerDemo 应用代码理解
  * Rules 是在 Driver 端进行实例化的.但是执行逻辑在Executor 中的Task 中执行的,所以需要序列化
  */
class Rules extends Serializable {
  val rulesMap = Map("hadoop" -> 2.7, "spark" -> 2.2)
}

//  第三种实现:
//  直接使用 Object ,还是在 Driver 端中进行实例化,然后Executor 端引用,这种方式,每个Executor 中的Task 共用一个规则对象
//  这里无需再使用序列化,因为Object 在类加载的时候就会进行实例化.
object Rules1 {
  val rulesMap = Map("hadoop" -> 2.7, "spark" -> 2.2)
}

//第四种:
// 由于Object 相当于静态的类,里面的方法相当于静态方法,在Excutor 端的代码中直接使用 Rules1.rulesMap 进行操作.
// 这样就免去了从 Driver 端传统规则的过程.

//第五种: 使用Redis 存规则数据.

// 上面的方式引申出来一个问题,我们也可以使用广播的方式实现这个功能.但是,广播功能里面存储的数据是不能发生变化的,比较有局限性.
// 使用上面规则的方式,可以定义一个定时器,定时更新规则,但是需要考虑锁的问题.
// 最好采用Redis 来做,天然支持这些