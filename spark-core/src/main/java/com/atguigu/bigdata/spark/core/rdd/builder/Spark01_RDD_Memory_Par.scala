package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author chulang
 * @date 2021/5/15
 * @description RDD并行
 */
object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    sparkConf.set("spark.default.parallelism","5")
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //RDD的并行度&分区
    //makeRDD方法第二个参数numSlices，表示分区的数量
    //第二个参数可以不传，会使用默认值：defaultParallelism（默认并行数）
//    val rdd: RDD[Int] = sc.makeRDD(
//      List(1, 2, 3, 4), numSlices = 2
//    )

    //测试默认并行数，取上面初始化环境中配置的核数
    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4)

    )
    //将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")


    //TODO 关闭环境
    sc.stop()

  }

}
