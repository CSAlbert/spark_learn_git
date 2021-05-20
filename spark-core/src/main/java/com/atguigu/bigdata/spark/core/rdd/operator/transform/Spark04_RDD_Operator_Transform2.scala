package com.atguigu.bigdata.spark.core.rdd.operator.transform

import java.util.ListResourceBundle

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author chulang
 * @date 2021/5/17
 * @description 小实例：List(List(1,2),3,List(4,5))扁平化
 */
object Spark04_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子 -flatMap
    val rdd = sc.makeRDD(List(
      List(List(1, 2), 3, List(4, 5))
    ))

    val flatRDD: RDD[Any] = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case data => List(data)
        }
      }
    )

    flatRDD.collect().foreach(println)

    sc.stop()

  }
}
