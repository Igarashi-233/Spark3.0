package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object
Spark04_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子---faltMap
    val rdd: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))

    val flatRDD: RDD[Any] = rdd.flatMap {
      case list: List[_] => list
      case other => List(other)
    }

    flatRDD.collect().foreach(println)

    sc.stop()

  }
}
