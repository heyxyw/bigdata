package com.zhouq.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * csv 数据源
  */
object CsvDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("CsvDataSource")
      .master("local[*]")
      .getOrCreate()

    //指定以后读取json类型的数据
    val csv: DataFrame = spark.read.csv("/Users/zx/Desktop/csv")

    csv.printSchema()

    val pdf: DataFrame = csv.toDF("id", "name", "age")

    pdf.show()

    spark.stop()


  }
}
