package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author chulang
 * @date 2021/5/17
 * @description 小实例：获取每个分区中的最大值
 */
object Spark02_RDD_Operator_Transform_test {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    //TODO 算子 -mapPartitions
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 各分区取最大值：【1，2】，【3，4】 =》 【2】，【4】
    // 使用map算子较难实现，map是一个一个读取，所以使用mapPartitions算子，按分区为单位进行数据获取，就能取到各个分区的最大值

    val mapRDD: RDD[Int] = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )
    mapRDD.collect().foreach(println)



    sc.stop()

  }
}
