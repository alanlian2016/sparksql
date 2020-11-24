package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author alanlian
 * @create 2020-09-29 15:01
 */
object SparkSQL05_UDF {
  def main(args: Array[String]): Unit = {
    // 1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")
    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 3 读取数据
    val df: DataFrame = spark.read.json("input/user.json")
    // 4 创建DataFrame临时视图
    df.createOrReplaceTempView("user")
    // 5 注册UDF函数。功能：在数据前添加字符串“Name:”
    spark.udf.register("addName",(x:String)=>"Name:"+x)
    // 6 调用自定义UDF函数
    spark.sql("select addName(name),age from user").show()
    // 7 释放资源
    spark.stop()

  }

}
