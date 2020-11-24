package com.atguigu.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author alanlian
 * @create 2020-09-29 15:10
 */
object SparkSQL02_RDDAndDataFrame {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)
    //3.1 获取数据
    val LineRDD: RDD[String] = sc.textFile("input/user.txt")
    //3.2 RDD准备完成
    val userRDD: RDD[(String, Int)] = LineRDD.map {
      line => {
        val fields: Array[String] = line.split(",")
        (fields(0), fields(1).trim.toInt)
      }
    }
    //4. 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //5.1 RDD和DataFrame、DataSet转换必须要导的包
    import spark.implicits._
    //5.3 RDD转换为DataFrame（通过样例类反射转）
    val userDataFrame: DataFrame = userRDD.map {
      case (name, age) => User(name, age)
    }.toDF()

    userDataFrame.show()
    //5.4 DataFrame 转换为RDD
    val userRDD1: RDD[Row] = userDataFrame.rdd
    userRDD1.collect().foreach(println)

    //4.关闭连接
    spark.stop()
    sc.stop()

  }

}
case class User(name: String, age: Long)