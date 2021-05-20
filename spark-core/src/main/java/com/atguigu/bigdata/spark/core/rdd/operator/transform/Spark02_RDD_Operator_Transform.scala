package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author chulang
 * @date 2021/5/17
 * @description map方法性能较低，因为是一个一个做操作，利用批处理提高性能->迭代器
 */
object Spark02_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    //TODO 算子 -mapPartitions
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    //mapPartitions：可以以分区为单位进行数据转换操作
    //               但是会将整个分区的数据加载到内存进行引用
    //               如果处理完的数据是不会被释放掉的，存在对象的引用
    //               在内存较小，数据量较大的场合下，容易出现内存溢出。此时可以考虑map算子，虽然速度慢点，但是不会报错

    val mapRDD: RDD[Int] = rdd.mapPartitions(
      iter => {
        println(">>>>>>>>>")
        iter.map(_ * 2)
      }
    )
    mapRDD.collect().foreach(println)


    sc.stop()

  }
}
