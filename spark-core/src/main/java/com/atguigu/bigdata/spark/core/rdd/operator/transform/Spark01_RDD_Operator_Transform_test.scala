package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author chulang
 * @date 2021/5/17
 * @description 小实例：从服务器日志数据apache.log中获取用户请求URL资源路径
 */
object Spark01_RDD_Operator_Transform_test {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    //TODO 算子 -map
    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    //长的字符串 =>短的字符串
    val mapRDD: RDD[String] = rdd.map(
      line => {
        val datas: Array[String] = line.split(" ")
        datas(6)
      }
    )
    mapRDD.collect().foreach(println)


    sc.stop()

  }
}
