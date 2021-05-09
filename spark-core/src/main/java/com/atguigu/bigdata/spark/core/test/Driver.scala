package com.atguigu.bigdata.spark.core.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

/**
 *
 * @author chulang
 * @date 2021/5/9
 * @description TODO
 */
object Driver {

  def main(args: Array[String]): Unit = {

    //连接服务器
    val client = new Socket("localhost", 9999)

    val out: OutputStream = client.getOutputStream

    val objOut = new ObjectOutputStream(out)

//    out.write(2)
//    out.flush()
//    out.close()

    val task = new Task()
    objOut.writeObject(task)
    objOut.flush()
    objOut.close()

    client.close()

    println("客户端数据发送完毕")
  }

}
