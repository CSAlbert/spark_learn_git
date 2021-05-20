package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author chulang
 * @date 2021/5/17
 * @description flatMap->扁平映射
 */
object Spark04_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子 -flatMap
    val rdd: RDD[String] = sc.makeRDD(List(
      "hello Scala", "hello Spark"
    ))
    val flatRDD: RDD[String] = rdd.flatMap(
      s => {
        s.split(" ")
      }
    )
    flatRDD.collect().foreach(println)

    sc.stop()

  }
}
