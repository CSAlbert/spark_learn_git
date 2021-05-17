package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author chulang
 * @date 2021/5/15
 * @description TODO
 */
object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //从文件中创建RDD，将文件中数据作为处理的数据源

    //path路径默认以当前环境的根路径作为基准，可以写绝对路径，也可以写相对路径
    //sc.textFile(path = "/Users/chulang/learn/hadoop/spark/idea/classes/atguigu-classes/datas/1.txt")
    //val rdd: RDD[String] = sc.textFile("datas/1.txt")

    //path路径可以是文件的具体路径，也可以是目录
    //val rdd: RDD[String] = sc.textFile("datas")

    //path路径还可以使用通配符*
    val rdd: RDD[String] = sc.textFile("datas/1*.txt")

    //path还可以是分布式存储系统路径：HDFS
    //val rdd: RDD[String] = sc.textFile("hdfs://hadoop102:8020/test.txt")

    rdd.collect().foreach(println)


    //TODO 关闭环境
    sc.stop()

  }

}
