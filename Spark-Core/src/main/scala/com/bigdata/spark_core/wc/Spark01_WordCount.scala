package com.bigdata.spark_core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {

    // TODO 建立与Spark框架的连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // TODO 执行业务操作
    val lines: RDD[String] = sc.textFile("data")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val tuples: RDD[(String, Int)] = words.map((_, 1))

    val result: RDD[(String, Int)] = tuples.reduceByKey(_ + _)

    result.collect().foreach(println(_))

    // TODO 关闭连接
    sc.stop()

  }
}
