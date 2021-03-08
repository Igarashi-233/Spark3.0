package com.bigdata.spark_core.test

class Task extends Serializable {

  var datas: List[Int] = _

  var logic: Int => Int = _

  def compute(): Seq[Int] = {
    datas.map(logic)
  }

}
