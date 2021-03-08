package com.bigdata.spark_core.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {
    // 连接服务器
    val client1 = new Socket("localhost", 9999)
    val client2 = new Socket("localhost", 8888)

    val dataStruct = new DataStruct

    val out1: OutputStream = client1.getOutputStream
    val objOUT1 = new ObjectOutputStream(out1)

    val subTask1 = new Task
    subTask1.datas = dataStruct.datas.take(2)
    subTask1.logic = dataStruct.logic

    objOUT1.writeObject(subTask1)
    objOUT1.flush()
    objOUT1.close()

    client1.close()


    val out2: OutputStream = client2.getOutputStream
    val objOUT2 = new ObjectOutputStream(out2)

    val subTask2 = new Task
    subTask2.datas = dataStruct.datas.takeRight(2)
    subTask2.logic = dataStruct.logic

    objOUT2.writeObject(subTask2)
    objOUT2.flush()
    objOUT2.close()

    client2.close()

    println("客户端数据发送完毕")

  }
}
