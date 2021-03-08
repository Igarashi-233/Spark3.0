package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object
Spark01_RDD_Operator_Transform_TestApacheLog {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子---Map
    val LogRDD: RDD[String] = sc.textFile("data/apache.log")

    val urlRDD: RDD[String] = LogRDD.map(Log => {
      val arrays: Array[String] = Log.split(" ")
      arrays(6)
    })

    urlRDD.collect().foreach(println)

    sc.stop()

  }
}
