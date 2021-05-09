package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 *
 * @author chulang
 * @date 2021/5/5
 * @description TODO
 */
object Spark03_WordCount {
  def main(args: Array[String]): Unit = {

    //TODO 建立和Spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //TODO 执行业务操作
    val lines: RDD[String] = sc.textFile(path = "datas")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    //hello，world，hello，spark，hello,world => (hello,1),(hello,1)...(spark,1)...
    val wordToOne = words.map {
      word => (word, 1)
    }

    //Spark框架提供了更多的功能，可以将分组和聚合使用一个方法实现
    //   旧代码
    //    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(
    //      t => t._1
    //    )
    //
    //
    //    val wordToCount = wordGroup.map {
    //      case (word, list) => {
    //        list.reduce(
    //          (t1,t2)=>{
    //            (t1._1,t1._2+t2._2)
    //          }
    //        )
    //      }
    //    }

    //reduceByKey：相同的key的数据，可以对value进行reduce聚合
    //    wordToOne.reduceByKey((x, y) => {x + y})
    //    wordToOne.reduceByKey((x, y) => x + y)
    val wordToCount = wordToOne.reduceByKey(_ + _)


    val array: Array[(String, Int)] = wordToCount.collect()

    array.foreach(println)

    sc.stop()

  }

}
