package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
 * @author alanlian
 * @create 2020-09-30 11:41
 */
object SparkSQL13_TopN {
  def main(args: Array[String]): Unit = {
    //1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

    //2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    spark.sql("use default")
    // 0 注册自定义聚合函数
    spark.udf.register("city_remark", functions.udaf(new CityRemarkUDAF()))
    // 1. 查询出所有的点击记录，并和城市表产品表做内连接
    spark.sql(
      """
        | select
        |    c.area, --地区
        |    c.city_name, -- 城市
        |    p.product_name, -- 商品
        |    v.click_product_id -- 点击商品id
        | from user_visit_action v
        | join city_info c
        | on v.city_id = c.city_id
        | join product_info p
        | on v.click_product_id = p.product_id
        | where click_product_id > -1
        |""".stripMargin).createOrReplaceTempView("t1")
    // 2. 分组计算每个区域，每个产品的点击量
    spark.sql(
      """
        |select
        |    t1.area, --地区
        |    t1.product_name, -- 商品名称
        |    count(*) click_count, -- 商品点击次数
        |    city_remark(t1.city_name) --城市备注
        |from t1
        |group by t1.area, t1.product_name
            """.stripMargin).createOrReplaceTempView("t2")

    // 3. 对每个区域内产品的点击量进行倒序排列
    spark.sql(
      """
        |select
        |    *,
        |    rank() over(partition by t2.area order by t2.click_count desc) rank -- 每个区域内按照点击量，倒序排行
        |from t2
            """.stripMargin).createOrReplaceTempView("t3")

    // 4. 每个区域取top3
    spark.sql(
      """
        |select
        |    *
        |from t3
        |where rank <= 3
            """.stripMargin).show

    //5 释放资源
    spark.stop()

  }

}

case class Buffer(var totalcnt: Long, var cityMap: mutable.Map[String, Long])


class CityRemarkUDAF extends Aggregator[String, Buffer, String] {

  override def zero: Buffer = Buffer(0L, mutable.Map[String, Long]())


  override def reduce(buffer: Buffer, city: String): Buffer = {
    // 总点击次数
    buffer.totalcnt += 1
    // 每个城市的点击次数
    val newCount: Long = buffer.cityMap.getOrElse(city, 0L) + 1
    buffer.cityMap.update(city, newCount)
    buffer
  }

  override def merge(b1: Buffer, b2: Buffer): Buffer = {
    // 合并所有城市的点击数量的总和
    b1.totalcnt += b2.totalcnt
    // 合并城市Map(2个Map合并)
    b2.cityMap.foreach {
      case (city, count) => {
        val newCount: Long = b1.cityMap.getOrElse(city, 0L) + count
        b1.cityMap.update(city, newCount)
      }
    }
    b1
  }

  override def finish(buffer: Buffer): String = {
    val remarkList = ListBuffer[String]()
    // 将统计的城市点击数量的集合进行排序，并取出前两名
    val cityCountList: List[(String, Long)] = buffer.cityMap.toList.sortWith {
      (Left, Right) => {
        Left._2 > Right._2
      }
    }.take(2)
    var sum: Long = 0L
    // 计算出前两名的百分比
    cityCountList.foreach {
      case (city, cnt) => {
        val r: Long = cnt * 100 / buffer.totalcnt
        remarkList.append(city + " " + r + "%")
        sum += r
      }
    }
    // 如果城市个数大于2，用其他表示
    if (buffer.cityMap.size > 2) {
      remarkList.append("其他" + (100-sum) + "%")

    }
    remarkList.mkString(",")

  }

  override def bufferEncoder: Encoder[Buffer] = Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}