package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author chulang
 * @date 2021/5/15
 * @description RDD并行
 */
object Spark01_RDD_Memory_Par1 {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD

    //查看源码，确认分配逻辑
    //不同分区数的分配方式
    //[1,2],[3,4]
    //    val rdd: RDD[Int] = sc.makeRDD(
    //      List(1, 2, 3, 4), numSlices = 2
    //    )


    //[1],[2],[3,4]
    //      val rdd: RDD[Int] = sc.makeRDD(
    //        List(1, 2, 3, 4), numSlices = 3
    //      )

    //[1],[2,3],[4,5]
    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4, 5), numSlices = 3
    )


    //将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")


    //TODO 关闭环境
    sc.stop()

  }

}
