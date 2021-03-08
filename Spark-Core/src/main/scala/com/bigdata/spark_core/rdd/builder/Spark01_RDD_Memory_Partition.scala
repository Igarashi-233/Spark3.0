package com.bigdata.spark_core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Partition {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // RDD并行度 & 分区
    // makeRDD可以设置分区数 默认 defaultParallelism()
    // scheduler.conf.getInt("spark.default.parallelism", totalCores) 默认从配置对象中获取配置参数
    //    如果没有设置 则使用 totalCores(当前运行环境最大可用核数)
    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4), 3
    )

    // 将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()


  }
}
