package com.bigdata.spark_core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object Spark02_RDD_File_Partition_Test {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // PartitionTest.txt 一共有19个字节
    // 19 / 2 = 9
    // 19 / 9 = 2....1(1.1) => 3(分区)
    /*
          123@@  => 0 1 2 3 4
          654@@  => 5 6 7 8 9
          789@@  => 10 11 12 13 14
          2333   => 15 16 17 18 19
     */
    // 0 => [0, 0+9] => 123 654
    // 1 => [9, 9+9] => 789 2333
    // 2 => [18, 18+1] => null


    val rdd: RDD[String] = sc.textFile("data/PartitionTest.txt", 2)

    rdd.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()


  }
}
