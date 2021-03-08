package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object
Spark01_RDD_Operator_Transform_Partition {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子---Map
    // 1. rdd的计算一个分区内的数据是一个一个执行逻辑
    //    只有前面一个数据全部的逻辑执行完毕后 才会执行下一个数据
    //    分区内数据的执行是有序的
    //2. 不同分区数据计算是无序的
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    val mapRDD: RDD[Int] = rdd.map(num => {
      println(">>>>>> " + num)
      num
    })

    val mapRDD1: RDD[Int] = mapRDD.map(num => {
      println("###### " + num)
      num
    })

    mapRDD1.collect()

    sc.stop()

  }
}
