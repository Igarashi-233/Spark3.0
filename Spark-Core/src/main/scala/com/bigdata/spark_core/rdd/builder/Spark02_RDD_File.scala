package com.bigdata.spark_core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // 从文件中创建RDD 将文件中的数据作为处理的数据源
    // path路径默认以当前环境的根路径为基准 可以写绝对路径 也可以写相对路径
    /* sc.textFile("D:\\IGRASHI\\IDEA-workspace\\Spark3.0\\data\\1.txt")  绝对路径 */
    /* val rdd: RDD[String] = sc.textFile("data/1.txt") 相对路径 */

    // path路径可以是文件的具体路径 也可以是目录名称(读取整个目录下的文件进行读取)
    /* val rdd: RDD[String] = sc.textFile("data") */

    // path路径可以使用通配符
    val rdd: RDD[String] = sc.textFile("data/1*.txt")

    // path可以使用分布式存储系统路径: HDFS
    /* sc.textFile("hdfs://hadoop102:9000/test.txt") */

    rdd.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()


  }
}
