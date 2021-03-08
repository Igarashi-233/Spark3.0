package com.bigdata.spark_core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object Spark02_RDD_File_Partition {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // textFile可以将文件作为数据处理的数据源 默认可以设定最小分区 minPartitions
    //    minPartitions: 最小分区数
    //    math.min(defaultParallelism, 2)
    /* val rdd: RDD[String] = sc.textFile("data/1.txt") */

    // Spark读取文件 底层采用Hadoop的读取方式
    // 分区数量计算方式:
    //    totalSize: 文件字节数总和
    //    goalSize: 文件字节数总和 / minPartitions
    //    totalSize / goalSize (1.1)+1
    val rdd: RDD[String] = sc.textFile("data/1.txt", 2)

    rdd.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()


  }
}
