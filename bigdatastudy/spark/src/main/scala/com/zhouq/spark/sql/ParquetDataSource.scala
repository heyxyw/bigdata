package com.zhouq.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * parquet 数据源
  *
  * parquet 数据格式介绍: https://www.infoq.cn/article/in-depth-analysis-of-parquet-column-storage-format
  */
object ParquetDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ParquetDataSource")
      .master("local[*]")
      .getOrCreate()

    //指定以后读取json类型的数据
    val parquetLine: DataFrame = spark.read.parquet("/Users/zx/Desktop/parquet")
    //val parquetLine: DataFrame = spark.read.format("parquet").load("/Users/zx/Desktop/pq")

    parquetLine.printSchema()

    //show是Action
    parquetLine.show()

    spark.stop()


  }
}
