package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author alanlian
 * @create 2020-09-29 12:42
 */
object sparksql {
  def main(args: Array[String]): Unit = {
    // 1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")
    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 3 读取数据
    val df: DataFrame = spark.read.json("input/user.json")

    // 4 可视化
    df.show()

    // 5 释放资源
    spark.stop()


  }

}
