package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author chulang
 * @date 2021/5/15
 * @description TODO
 */
object Spark02_RDD_File_Par {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //textFile可以将文件作为数据处理的数据源，默认也可以设定分区
    //  minPartition ：最小分区数量
    //  math.min(defaultParallelism,2)
    //    val rdd: RDD[String] = sc.textFile("datas/11.txt")

    //如果不想使用默认分区数量，就可以通过第二个参数进行指定
    //spark读取文件，底层其实使用的就是hadoop的读取方式
    //分区数量的计算方式：
    //    totalSize = 7
    //    goalSize = 7/2=3（byte）

    //    7/3 = 2...1 + 1 = 3(分区）
    val rdd: RDD[String] = sc.textFile("datas/22.txt", 2)

    //TODO 数据分区的分配
    //1、数据以行为单位进行读取
    //   spark读取文件，采用的是hadoop的方式读取，所以一行一行读取，和字节数没有关系
    //2、数据读取时以偏移量为单位，偏移量不会被重复读取
    /*
      1@@   => 012
      2@@   => 345
      3   => 6
    */
    //3、数据分区的偏移量范围的计算
    // 0 => [0,3] =>12
    // 1 => [3,6] =>3
    // 2 => [6,7] =>

    // 三个分区内容【1，2】、【3】、【】
    rdd.saveAsTextFile("output/output01")

    //TODO 关闭环境
    sc.stop()

  }

}
