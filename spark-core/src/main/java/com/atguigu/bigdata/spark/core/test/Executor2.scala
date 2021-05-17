package com.atguigu.bigdata.spark.core.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

/**
 *
 * @author chulang
 * @date 2021/5/9
 * @description TODO
 */
object Executor2 {

  def main(args: Array[String]): Unit = {

    //启动服务器，接收数据
    val server = new ServerSocket(8888)
    println("服务器启动，等待接收数据")

    //等待客户端的连接
    val client: Socket = server.accept()

    val in: InputStream = client.getInputStream

    val objIn = new ObjectInputStream(in)

    //    val i: Int = in.read()
    val task: SubTask = objIn.readObject().asInstanceOf[SubTask]
    val ints: List[Int] = task.compute()

    //    println("接收到客户端发送的数据：" + i)
    println("计算节点[8888]计算的结果为：" + ints)
//    in.close()
    objIn.close()
    client.close()
    server.close()
  }

}
