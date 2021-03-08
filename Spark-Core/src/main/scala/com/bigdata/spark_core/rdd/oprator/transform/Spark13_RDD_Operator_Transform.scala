package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object
Spark13_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子---sortBy 会造成双Value类型
    // 交集 并集 差集要求数据类型保持一致
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))
    val rdd7: RDD[String] = sc.makeRDD(List("asd", "dsa", "sda", "sad"))

    // 交集
    val rdd3: RDD[Int] = rdd1.intersection(rdd2)
    println(rdd3.collect().mkString(","))

    // 并集
    val rdd4: RDD[Int] = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))

    // 差集
    val rdd5: RDD[Int] = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))

    // 拉链 数据类型可以不同
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))

    val rdd8: RDD[(Int, String)] = rdd1.zip(rdd7)
    println(rdd8.collect().mkString(","))

    sc.stop()

  }
}
