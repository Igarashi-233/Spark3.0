package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object
Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子--- Key-Value类型
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))

    // RDD => PairRDDFunction
    // partitionBy: 根据指定分区规则对数据进行重分区
    mapRDD.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")

    sc.stop()

  }
}
