package com.bigdata.spark_core.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor2 {
  def main(args: Array[String]): Unit = {
    // 启动服务器 接收数据
    val server = new ServerSocket(8888)
    println("服务器启动 等待接收数据")

    // 等待客户端连接
    val client: Socket = server.accept()

    val in: InputStream = client.getInputStream

    val objIN = new ObjectInputStream(in)

    val task: Task = objIN.readObject().asInstanceOf[Task]
    val ints: Seq[Int] = task.compute()

    println("计算节点[8888]计算的结果为: " + ints)

    objIN.close()
    client.close()
    server.close()

  }
}
