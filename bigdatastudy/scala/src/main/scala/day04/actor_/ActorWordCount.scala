package com.zhouq.scala.day04.actor_

import scala.actors.{Actor, Future}
import scala.collection.mutable.{HashSet, ListBuffer}
import scala.io.Source

/**
  * Create by zhouq on 2019/1/24
  *
  */

class Task extends Actor {
  override def act(): Unit = {
    loop {
      react {
        case SubmitTask(fileName) => {
          //先取出每一行进行局部求和map("hello" -> 2)
          // 加载文件
          val result = Source.fromFile(fileName)
            //读取每一行 a a xx xxx xxxxx
            .getLines()
            //进行分割并拍扁  (a,1),(a,1),(xx,1),(xxx,1).....
            .flatMap(_.split(" ")).map((_, 1))
            //List((a,1),(a,1),(xx,1),(xxx,1))
            .toList
            // 按元祖的第一个元素进行分组 Map("a" -> List((a,1),(a,1)),"xx" ->List(xx,1),......)
            .groupBy(_._1)
            //按value 进行求总数 Map("a" ->2,"xx" ->1,"xxx" ->1)
            .mapValues(_.size)

          //把数据封装好发送出去
          sender ! ResultTask(result)
        }
        case StopTask => {
          exit()
        }
      }
    }
  }
}

case class SubmitTask(fileName: String)

case class ResultTask(result: Map[String, Int])

case object StopTask


object ActorWordCount {
  def main(args: Array[String]): Unit = {
    // 存放actor 返回值 Future
    var futureSet = new HashSet[Future[Any]]()

    //准备存放 resultTask 集合

    var resultTaskList = new ListBuffer[ResultTask]()

    //准备文件名列表
    val fileNames = Array[String]("G:\\stady\\scalatest\\a.txt", "G:\\stady\\scalatest\\b.log")

    //处理文件
    for (fileName <- fileNames) {
      val task = new Task

      //启动线程并提交任务,返回一个 Future
      val future = task.start() !! SubmitTask(fileName)

      // 把返回的对象存入集合中
      futureSet += future
    }


    // 循环处理结果集

    while (futureSet.size > 0) {
      // 先过滤可以处理的任务

      var toCompiled = futureSet.filter(_.isSet) //.isSet 方法表示任务可以处理

      for (f <- toCompiled) {
        //apply 方法表示获取结果
        val resultTask = f.apply().asInstanceOf[ResultTask]
        resultTaskList += resultTask
        // 把 处理完成就把 future U移除
        futureSet -= f
      }

    }

    //处理最后结果 reduce 阶段

    val fr = resultTaskList.flatMap(_.result).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2)).toList.sortBy(x => -x._2)

    println(fr)
  }

}

