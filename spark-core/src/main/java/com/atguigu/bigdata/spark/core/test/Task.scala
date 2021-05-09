package com.atguigu.bigdata.spark.core.test

/**
 *
 * @author chulang
 * @date 2021/5/9
 * @description TODO
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
