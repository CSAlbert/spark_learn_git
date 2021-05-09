package com.atguigu.bigdata.spark.core.test

/**
 *
 * @author chulang
 * @date 2021/5/9
 * @description 数据结构（封装所有数据和处理逻辑的类），真正处理处理的是subTask
 */
class Task extends Serializable {

  val datas = List(1, 2, 3, 4)

  //  val logic = (num: Int) => {
  //    num * 2
  //  }

  val logic: (Int) => Int = _ * 2

  //计算
  def compute() = {
    datas.map(logic)
  }

}
