package com.bigdata.spark_core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File1 {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // 从文件中创建RDD 将文件中的数据作为处理的数据源
    // textFile: 以行为单位读取数据
    // wholeTextFiles: 以文件为单位读取数据(可以拿到文件路径)
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("data")

    rdd.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()


  }
}
