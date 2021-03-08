package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object
Spark07_RDD_Operator_Transform_TestApacheLog {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子---filter
    val rdd: RDD[String] = sc.textFile("data/apache.log")

    rdd.filter(line => {
      val data: Array[String] = line.split(" ")
      val time: String = data(3)
      time.startsWith("17/05/2015")
    }).collect().foreach(println)

    sc.stop()

  }
}
