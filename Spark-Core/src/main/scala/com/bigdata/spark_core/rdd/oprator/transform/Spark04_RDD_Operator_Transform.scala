package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object
Spark04_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子---faltMap
    val rdd: RDD[List[Int]] = sc.makeRDD(List(
      List(1, 3, 5), List(2, 4, 6)
    ))
    val flatRDD: RDD[Int] = rdd.flatMap(list => {
      list.map(_ * 2)
    })

    flatRDD.collect().foreach(println)

    sc.stop()

  }
}
