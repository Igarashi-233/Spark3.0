package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object
Spark15_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子--- Key-Value类型
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3), ("c", 4)
    ),4)

    // reduceByKey : 相同的key的数据进行value数据的聚合操作
    // scala语言中一般的聚合操作都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合
    // 【1, 2, 3】
    // 【3, 3】
    // 【6】
    // reduceByKey中如果key的数据只有一个，是不会参与运算的。
    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
      println(s"x = $x, y = $y")
      x + y
    })

    reduceRDD.collect().foreach(println)

    sc.stop()

  }
}
