package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author alanlian
 * @create 2020-09-29 19:37
 */
object SparkSQL10_MySQL_Read {
  def main(args: Array[String]): Unit = {
    //1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

    //2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 3.1 通用的load方法读取
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/gmall")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user_info")
      .load()
    // 3.2 创建视图
    df.createOrReplaceTempView("user")

    // 3.3 查询想要的数据
    spark.sql("select id, name from user").show()


    //4 释放资源
    spark.stop()

  }

}
