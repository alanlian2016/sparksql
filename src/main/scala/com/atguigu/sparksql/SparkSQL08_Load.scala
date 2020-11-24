package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author alanlian
 * @create 2020-09-29 19:08
 */
object SparkSQL08_Load {
  def main(args: Array[String]): Unit = {
    //1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

    //2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 3.1 spark.read直接读取数据：csv   format   jdbc   json   load   option
    // options   orc   parquet   schema   table   text   textFile
    // 注意：加载数据的相关参数需写到上述方法中，
    // 如：textFile需传入加载数据的路径，jdbc需传入JDBC相关参数。
    spark.read.json("input/user.json").show()
    // 3.2 format指定加载数据类型
    // 3.2 format指定加载数据类型
    // spark.read.format("…")[.option("…")].load("…")
    // format("…")：指定加载的数据类型，包括"csv"、"jdbc"、"json"、"orc"、"parquet"和"textFile"
    // load("…")：在"csv"、"jdbc"、"json"、"orc"、"parquet"和"textFile"格式下需要传入加载数据路径
    // option("…")：在"jdbc"格式下需要传入JDBC相应参数，url、user、password和dbtable
    spark.read.format("json").load("input/user.json").show()

    //4 释放资源
    spark.stop()

  }

}
