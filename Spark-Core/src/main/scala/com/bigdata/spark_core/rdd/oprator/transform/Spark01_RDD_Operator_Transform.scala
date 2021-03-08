package com.bigdata.spark_core.rdd.oprator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object
Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Operator").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO 算子---Map
    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 3, 5), List(2, 4, 6)))
    //1,2,3,4,5 => 2,4,6,8,10

    /*
        def mapFunction(num: Int): Int = {
          num * 2
        }

        val mapRDD: RDD[Int] = rdd.map(mapFunction)
    */

    // val mapRDD: RDD[Int] = rdd.map((num:Int)=>{num*2})
    val mapRDD: RDD[List[Int]] = rdd.map(list => {
      list.map(_ * 2)
    })

    mapRDD.collect().foreach(println)

    sc.stop()

  }
}
