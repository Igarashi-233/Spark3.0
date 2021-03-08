package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object
Spark02_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子---mapPartitions
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // mapPartitions: 可以以分区为单位进行数据转换操作
    //                但是会将整个分区的数据全部加载到内存进行引用
    //                处理完的数据不会被释放(存在数据引用)
    //                在内存小 数据量大的情况下会出现 OOM
    val mapParRDD: RDD[Int] = rdd.mapPartitions(iter => {
      println(">>>>>>>>")
      iter.map(_ * 2)
    })

    mapParRDD.collect().foreach(println)

    sc.stop()

  }
}
