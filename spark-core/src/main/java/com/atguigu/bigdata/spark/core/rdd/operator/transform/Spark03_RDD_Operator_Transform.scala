package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author chulang
 * @date 2021/5/17
 * @description 小实例：获取第二个分区的数据
 */
object Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    //TODO 算子 -mapPartitionsWithIndex
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    // 获取第二个分区的数据：【1，2】，【3，4】，【5，6】
    // 使用mapPartitionsWithIndex方法利用index实现

    val mpiRDD: RDD[Int] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }
      }
    )
    mpiRDD.collect().foreach(println)


    sc.stop()

  }
}
