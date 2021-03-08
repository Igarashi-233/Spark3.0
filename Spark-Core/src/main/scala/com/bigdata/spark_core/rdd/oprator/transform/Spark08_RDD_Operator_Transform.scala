package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object
Spark08_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子---sample
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // sample算子需要传递三个参数
    // 1. 第一个参数表示 抽取后是否放回 true(放回)
    // 2. 第二个参数表示 1) 如果是抽取不放回 数据源中每条数据被抽取的概率
    //                2) 抽取放回 数据源中每条数据被抽取的可能次数
    //                基准值概念
    // 3. 第三个参数表示 抽取数据时随机算法的种子
    //                如果不传递第三个参数 默认使用当前系统时间
    println(rdd.sample(
      withReplacement = false,
      0.4,
      1
    ).collect().mkString(","))

    sc.stop()

  }
}
