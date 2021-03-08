package com.bigdata.spark_core.test

class DataStruct extends Serializable {

  val datas = List(1, 2, 3, 4)

  val logic: Int => Int = _ * 2

}
