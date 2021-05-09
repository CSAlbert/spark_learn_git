package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 *
 * @author chulang
 * @date 2021/5/5
 * @description TODO
 */
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {

    //Application
    //Spark框架

    //使用步骤如下（参考数据库的连接使用）
    //TODO 建立和Spark框架的连接
    //JDBC：Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")

    val sc = new SparkContext(sparkConf)

    //TODO 执行业务操作

    //1、读取文件，获取一行一行的数据
    // hello world， hello spark， hello world
    val lines: RDD[String] = sc.textFile(path = "datas")

    //2、将一行数据进行拆分，形成一个一个的单词（分词）
    // 扁平化：将整体拆分为个体的操作
    // "hello world ，hello spark， hello spark" =》hello，world，hello，spark，hello,world
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //3、将数据根据单词进行分组，便于统计
    //（hello，hello，hello），（world,world），（spark）
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    //4、对分组后的数据进行转换
    //（hello，hello，hello），（world,world），（spark） =》
    //（hello，3），（world，2），（spark，1）
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    //5、将转换结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordToCount.collect()

    array.foreach(println)

    //TODO 关闭连接
    sc.stop()

  }

}
