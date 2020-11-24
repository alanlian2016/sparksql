package com.atguigu.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author alanlian
 * @create 2020-09-29 15:26
 */
object SparkSQL03_RDDAndDataSet {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)
    //3.1 获取数据
    val LineRDD: RDD[String] = sc.textFile("input/user.txt")

    //4. 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //5.1 RDD和DataFrame、DataSet转换必须要导的包
    import spark.implicits._
    //5.2 RDD转换为DataSet
    val userDataSet: Dataset[User] = LineRDD.map {
      line =>
        val fields = line.split(",")
        User(fields(0), fields(1).toInt)
    }.toDS()
    userDataSet.show()
    //5.3 DataSet转换为RDD
    val userRDD: RDD[User] = userDataSet.rdd
    userRDD.collect().foreach(println)

    //4.关闭连接
    spark.stop()
    sc.stop()

  }

}
