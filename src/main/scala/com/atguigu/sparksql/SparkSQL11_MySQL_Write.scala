package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
 * @author alanlian
 * @create 2020-09-29 19:55
 */
object SparkSQL11_MySQL_Write {
  def main(args: Array[String]): Unit = {
    //1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

    //2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    // 3 准备数据
    // 注意：id是主键，不能和MySQL数据库中的id重复
    val rdd: RDD[User1] = spark.sparkContext.makeRDD(List(User1(30000, "zhangsan"), User1(30010, "lisi")))

    val ds: Dataset[User1] = rdd.toDS
    // 4 向MySQL中写入数据
    ds.write
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/gmall")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user_info")
      .mode(SaveMode.Append)
      .save()

    //5 释放资源
    spark.stop()

  }

}

case class User1(id: Int, name: String)