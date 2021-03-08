package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object
Spark04_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子---faltMap
    val rdd: RDD[String] = sc.makeRDD(List(
      "Hello Spark", "Hello Scala", "Hive Hadoop"
    ))

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    flatRDD.collect().foreach(println)

    sc.stop()

  }
}
