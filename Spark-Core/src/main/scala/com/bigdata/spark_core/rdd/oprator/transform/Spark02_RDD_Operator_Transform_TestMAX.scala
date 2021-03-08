package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object
Spark02_RDD_Operator_Transform_TestMAX {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子---mapPartitions
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // [1, 2] , [3, 4]
    // [2]    , [4]
    val mapParRDD: RDD[Int] = rdd.mapPartitions(iter => {
      List(iter.max).iterator
    })

    mapParRDD.foreach(println)

    sc.stop()

  }
}
