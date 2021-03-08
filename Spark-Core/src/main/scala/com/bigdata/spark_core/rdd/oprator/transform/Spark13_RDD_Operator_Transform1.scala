package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object
Spark13_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子---sortBy 会造成双Value类型
    // 交集 并集 差集要求数据类型保持一致
    // Can't zip RDDs with unequal numbers of partitions: List(2, 4) 两个数据源要求分区保持一致
    // Can only zip RDDs with same number of elements in each partition 分区中数据源数量要求一致
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6), 2)

    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))

    sc.stop()

  }
}
