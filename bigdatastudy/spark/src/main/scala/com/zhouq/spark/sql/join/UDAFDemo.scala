package com.zhouq.spark.sql.join

import java.lang

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 自定义 UDAF 实现几何平均数需求
  * 几何平均数: 1*2*3*4* ... *N  再开 n次方
  *
  */
object UDAFDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("UDAFDemo")
      .master("local[*]")
      .getOrCreate()

    //range 函数表示 取 [1,10) 的数字,步长为 1 即 (1,2,3,4,5,6,7,8,9,10)
    //range 会默认生成一个字段叫 id
    val range: Dataset[lang.Long] = spark.range(1, 11)

    val geoMean = new GeoMean

    //第一种方式: sql 方式计算
    //注册UDAF
    //    spark.udf.register("gm",geoMean)
    //    //range 会默认生成一个字段叫 id
    //    range.createTempView("v_range")
    //
    //    val r: DataFrame = spark.sql("select gm(id) result from v_range")

    //第二种: 采用DSL

    import spark.implicits._
    //使用自定义函数
    val r: DataFrame = range.agg(geoMean($"id").as("geoMean"))

    r.show()

    spark.stop()

  }
}

//继承 UDAF 函数
class GeoMean extends UserDefinedAggregateFunction {

  //表示输入数据的类型
  override def inputSchema: StructType = StructType(List(
    StructField("value", DoubleType)
  ))

  //表示产生中间结果的数据类型 第一个数表示积,第二个数表示参与运算的数字个数
  override def bufferSchema: StructType = StructType(List(
    //相乘之后返回的积
    StructField("product", DoubleType),
    //参与运算数字的个数
    StructField("counts", LongType)
  ))

  //最终返回数据的结果集类型
  override def dataType: DataType = DoubleType

  //确保一致性 一般用true
  override def deterministic: Boolean = true

  //制定初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //表示相乘积的初始值
    buffer(0) = 1.0
    //表示初始参与运算数字的个数
    buffer(1) = 0L
  }

  //表示有一条数据参与运算就更新一下中间结果(update 相当于在每一个分区中计算)
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //每有一个数字参与运算就进行相乘（包含中间结果）
    buffer(0) = buffer.getDouble(0) * input.getDouble(0)
    //参与运算数据的个数也进行更新
    buffer(1) = buffer.getLong(1) + 1
  }

  //全局聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //计算每个分区的结果进行相乘
    buffer1(0) = buffer1.getDouble(0) * buffer2.getDouble(0)
    //每个分区参与预算的中间结果进行相加
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //最终计算结果
  override def evaluate(buffer: Row): Double = {
    math.pow(buffer.getDouble(0), 1.toDouble / buffer.getLong(1))
  }
}
