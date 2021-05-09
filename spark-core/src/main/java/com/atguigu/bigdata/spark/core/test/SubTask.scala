package com.atguigu.bigdata.spark.core.test

/**
 *
 * @author chulang
 * @date 2021/5/9
 * @description TODO
 */
class SubTask extends Serializable {

  var datas: List[Int] = _

  var logic: (Int) => Int = _

  //计算
  def compute() = {
    datas.map(logic)
  }
}
