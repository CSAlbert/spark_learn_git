package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author chulang
 * @date 2021/5/15
 * @description TODO
 */
object Spark02_RDD_File_Par1 {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD

    //计算分区实例
    // word.txt总大小为14个字节
    //14/2 = 7byte
    //=> 14/7 = 2(分区）

    //分区数据分配如下

    /*
      数据偏移量:
      1234567@@ =>012345678
      89@@      =>9101112
      0         =>13

      分区包含值
      [0,7] => 1234567
      [7,14] => 890
    */

    val rdd: RDD[String] = sc.textFile("datas/word.txt", 2)

    rdd.saveAsTextFile("output/output02 ")

    //TODO 关闭环境
    sc.stop()

  }

}
