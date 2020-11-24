//package com.atguigu.sparksql
//
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.expressions.Aggregator
//import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}
//
///**
// * @author alanlian
// * @create 2020-09-30 8:42
// */
//object Spark_UDAF {
//  def main(args: Array[String]): Unit = {
//    //1 创建上下文环境配置对象
//    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")
//
//    //2 创建SparkSession对象
//    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
//
//    // 3 读取数据
//    val df: DataFrame = spark.read.json("input/user.json")
//
//    // 4 创建DataFrame临时视图
//    df.createOrReplaceTempView("user")
//
//    // 5 注册UDAF
//    //spark.udf.register("myAvg", functions.udaf(new MyAvg1()))
//    spark.udf.register("avg1",functions.udaf(new MyAvg1()))
//    // 6 调用自定义UDAF函数
//   // spark.sql("select myAvg(age) from user").show()
//    spark.sql("select avg1(age) from user").show
//
//
//    //4 释放资源
//    spark.stop()
//
//  }
//
//}
//
//case class Buffer(var sum: Long, var count: Long)
//
//
//class MyAvg1 extends Aggregator[Long, Buffer, Double] {
//  // 初始化缓冲区
//  override def zero: Buffer = Buffer(0L, 0L)
//
//  // 将输入的年龄和缓冲区的数据进行聚合
//  override def reduce(buffer: Buffer, age: Long): Buffer = {
//    buffer.sum += age
//    buffer.count += 1
//    buffer
//  }
//
//  // 多个缓冲区数据合并
//  override def merge(b1: Buffer, b2: Buffer): Buffer = {
//    b1.sum += b2.sum
//    b1.count += b2.count
//    b1
//  }
//
//  // 完成聚合操作，获取最终结果
//  override def finish(buffer: Buffer): Double = {
//    buffer.sum / buffer.count.toDouble
//  }
//
//  // 自定义类型就是product   自带类型根据类型选择
//  override def bufferEncoder: Encoder[Buffer] = Encoders.product
//
//  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
//}
