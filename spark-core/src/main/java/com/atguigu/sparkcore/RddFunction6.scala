package com.atguigu.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupBy算子
 */
object RddFunction6 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("RddFunction6")

    val sc = new SparkContext(conf)


    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8),4)

    val newRdd = rdd.groupBy(num => num % 2,2)

    newRdd.collect().foreach(println)

    sc.stop()
  }
}
