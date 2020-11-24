package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author alanlian
 * @create 2020-09-29 19:15
 */
object SparkSQL09_Save {
  def main(args: Array[String]): Unit = {
       //1 创建上下文环境配置对象
               val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

              //2 创建SparkSession对象
               val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 3 获取数据
    val df: DataFrame = spark.read.json("input/user.json")

    // 4.1 df.write.保存数据：csv  jdbc   json  orc   parquet textFile… …
    // 注意：保存数据的相关参数需写到上述方法中。如：textFile需传入加载数据的路径，JDBC需传入JDBC相关参数。
    // 默认保存为parquet文件（可以修改conf.set("spark.sql.sources.default","json")）
    df.write.save("output")

    // 默认读取文件parquet
    spark.read.load("output").show()

    // 4.2 format指定保存数据类型
    // df.write.format("…")[.option("…")].save("…")
    // format("…")：指定保存的数据类型，包括"csv"、"jdbc"、"json"、"orc"、"parquet"和"textFile"。
    // save ("…")：在"csv"、"orc"、"parquet"和"textFile"格式下需要传入保存数据的路径。
    // option("…")：在"jdbc"格式下需要传入JDBC相应参数，url、user、password和dbtable
    df.write.format("json").save("output2")
    // 4.3 可以指定为保存格式，直接保存，不需要再调用save了
    df.write.json("output1")
    // 4.4 如果文件已经存在则追加
    df.write.mode("append").json("output2")
    // 如果文件已经存在则忽略
    df.write.mode("ignore").json("output2")
    // 如果文件已经存在则覆盖
    df.write.mode("overwrite").json("output2")

    // 默认default:如果文件已经存在则抛出异常
    // path file:/E:/ideaProject2/SparkSQLTest/output2 already exists.;
    df.write.mode("error").json("output2")

    //4 释放资源
               spark.stop()

  }

}
