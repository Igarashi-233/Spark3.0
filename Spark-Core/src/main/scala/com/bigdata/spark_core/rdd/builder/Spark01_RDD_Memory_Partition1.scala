package com.bigdata.spark_core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Partition1 {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    /* val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 3) */
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 3)


    // 将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()


  }
}
