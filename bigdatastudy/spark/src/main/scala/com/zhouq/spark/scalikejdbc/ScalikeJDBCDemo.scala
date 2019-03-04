package com.zhouq.spark.scalikejdbc

import scalikejdbc._
import scalikejdbc.config.DBs

/**
  * ScalikeJDBC  demo
  *
  */
object ScalikeJDBCDemo {
  def main(args: Array[String]): Unit = {

    DBs.setup()

    // 查询数据并返回单个列, 并将列数据封装到集合中
    //    val list: List[String] = DB readOnly { implicit session =>
    //      sql"select name from user".map(rs =>
    //        rs.string("name")).list().apply()
    //    }
    //
    //    for (s <- list) {
    //      println(s)
    //    }

    // 查询数据封装对象
    //    val userses: List[User] = DB readOnly { implicit session =>
    //      sql"select id,name,age,fv from user".map(rs =>
    //        User(rs.long("id"), rs.string("name"), rs.int("age"), rs.int("fv"))
    //      ).list().apply()
    //    }
    //
    //    for (usr <- userses) {
    //      println(usr)
    //    }

    //    // 插入数据,使用autoCommit
    //    val insertResult: Int = DB.autoCommit {
    //      implicit session =>
    //        SQL("insert into user(name,age,fv) values (?,?,?)")
    //          .bind("dlrb", 25, 110)
    //          .update().apply()
    //    }
    //
    //    println(insertResult)


    //插入数据返回主键
    //    val id: Long = DB.autoCommit {
    //      implicit session =>
    //        SQL("insert into user(name,age,fv) values (?,?,?)")
    //          .bind("james", 33, 999)
    //          .updateAndReturnGeneratedKey("id").apply()
    //    }
    //    println(id)

    //    //事物插入
    //    val tx: Int = DB.localTx { implicit session =>
    //      sql"insert into user(name,age,fv) values (?,?,?)"
    //        .bind("xxoo", 15, 10).update().apply()
    //      var a = 1 / 0
    //      SQL("insert into user(name,age,fv) values (?,?,?)")
    //        .bind("xxoo1", 18, 68).update().apply()
    //    }
    //    println(s"tx = ${tx}")

    //更新数据
    val tx: Int = DB.localTx { implicit session =>
      sql"UPDATE user SET NAME = ? WHERE id = 7"
        .bind("memedada").update().apply()
    }
    println(s"tx = ${tx}")
  }

}

case class User(id: Long, name: String, age: Int, fv: Int) {
  override def toString: String = {
    s"id=${id},name=${name},age=${age},fv=${fv}"
  }
}
