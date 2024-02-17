package com.atguigu.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
 * flatMap算子
 */
object RddFunction5 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("RddFunction5")

    val sc   = new SparkContext(conf)

    val rdd = sc.makeRDD(List(List(1,2),3,List(4,5)))

    val newRdd = rdd.flatMap {
      case list: List[_] => list
      case other => List(other)
    }

    newRdd.collect().foreach(println)

  }
}
