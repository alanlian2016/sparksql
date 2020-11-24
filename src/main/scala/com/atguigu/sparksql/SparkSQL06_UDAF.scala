package com.atguigu.sparksql



import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

/**
 * @author alanlian
 * @create 2020-09-29 15:52
 */
object SparkSQL06_UDAF {
  def main(args: Array[String]): Unit = {
    //1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

    //2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3 读取数据
    val df: DataFrame = spark.read.json("input/user.json")

    // 4 创建DataFrame临时视图
    df.createOrReplaceTempView("user")

    // 5 注册UDAF
    spark.udf.register("myAvg", functions.udaf(new MyAvgUDAF()))
    // 6 调用自定义UDAF函数
    spark.sql("select myAvg(age) from user").show()

    //7 释放资源
    spark.stop()

  }

}

//输入数据类型
case class Buff(var sum: Long, var count: Long)

/**
 * 1,20岁； 2,19岁； 3,18岁
 * IN:聚合函数的输入类型：Long
 * BUF：
 * OUT:聚合函数的输出类型:Double  (18+19+20)/3
 */
class MyAvgUDAF extends Aggregator [Long,Buff,Double]{
  // 初始化缓冲区
  override def zero: Buff = Buff(0L,0L)
  // 将输入的年龄和缓冲区的数据进行聚合
  override def reduce(buff: Buff, age: Long): Buff = {
    buff.sum += age
    buff.count += 1
    buff
  }
  // 多个缓冲区数据合并
  override def merge(b1: Buff, b2: Buff): Buff = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  // 完成聚合操作，获取最终结果
  override def finish(buff: Buff): Double = {
    buff.sum.toDouble / buff.count
  }
  // 自定义类型就是product   自带类型根据类型选择
  override def bufferEncoder: Encoder[Buff] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

