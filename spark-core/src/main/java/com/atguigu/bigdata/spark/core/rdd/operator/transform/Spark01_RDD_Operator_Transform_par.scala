package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author chulang
 * @date 2021/5/17
 * @description 小实例：从服务器日志数据apache.log中获取用户请求URL资源路径
 */
object Spark01_RDD_Operator_Transform_par {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    //TODO 算子 -map
    //改变分区数，看并行执行情况

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    val mapRDD: RDD[Int] = rdd.map(
      num => {
        println("num>>>>>> = " + num)

        num
      }
    )

    val mapRDD1: RDD[Int] = mapRDD.map(
      num => {
        println("num####### = " + num)

        num
      }
    )

    mapRDD1.collect()

    sc.stop()

  }
}
