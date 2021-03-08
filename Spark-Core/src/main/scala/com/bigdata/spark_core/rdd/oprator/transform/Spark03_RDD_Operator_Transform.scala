package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object
Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子---mapPartitions
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // [1, 2] , [3, 4]
    val mapParIndexRDD: RDD[Int] = rdd.mapPartitionsWithIndex((index, iter) => {
      if (index == 1)
        iter
      else
        Nil.iterator
    })

    mapParIndexRDD.foreach(println)

    sc.stop()

  }
}
