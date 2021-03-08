package com.bigdata.spark_core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object Spark02_RDD_File_Partition1 {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // TODO 数据分区的分配
    // 1. 数据以行为单位读取
    //    Spark读取文件 采用Hadoop的方式读取 与字节无关
    // 2. 数据读取以偏移量为单位 偏移量不会重复读取
    /*
          1@@   => 012
          2@@   => 345
          3     => 6
     */
    // 3.数据分区的偏移量的范围计算
    //    0 => [0, 0+3] => 1 2
    //    1 => [3, 3+3] => 3
    //    2 => [6, 6+1] => null

    val rdd: RDD[String] = sc.textFile("data/1.txt", 2)

    rdd.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()


  }
}
