package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object
Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子---sortBy 会造成shuffle
    val rdd: RDD[Int] = sc.makeRDD(List(2, 1, 5, 6, 3, 4), 2)

    val newRDD: RDD[Int] = rdd.sortBy(num => num)

    newRDD.collect().foreach(println)

    sc.stop()

  }
}
