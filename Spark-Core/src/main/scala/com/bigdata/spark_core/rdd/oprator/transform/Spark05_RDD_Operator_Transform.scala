package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object
Spark05_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子---glom
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val glomRDD: RDD[Array[Int]] = rdd.glom()

    glomRDD.collect().foreach(data => println(data.mkString(",")))

    sc.stop()

  }
}
