package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @author alanlian
 * @create 2020-09-29 15:43
 */
object SparkSQL04_DataFrameAndDataSet {
  def main(args: Array[String]): Unit = {
    // 1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3 读取数据
    val df: DataFrame = spark.read.json("input/user.json")
    //4.1 RDD和DataFrame、DataSet转换必须要导的包
    import spark.implicits._
    // 4.2 DataFrame 转换为DataSet
    val userDataSet: Dataset[User] = df.as[User]
    userDataSet.show()
    // 4.3 DataSet转换为DataFrame
val userDataFrame: DataFrame = userDataSet.toDF()
    userDataFrame.show()

    // 5 释放资源
    spark.stop()
  }

}
