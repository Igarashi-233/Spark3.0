package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object
Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子---distinct
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 4)

    // coalesce方法默认情况下不会将分区的数据打乱重新组合
    // 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
    // 如果想要让数据均衡，可以进行shuffle处理
    //val newRDD: RDD[Int] = rdd.coalesce(2)
    val newRDD: RDD[Int] = rdd.coalesce(2, shuffle = true)

    newRDD.saveAsTextFile("output")

    sc.stop()

  }
}
