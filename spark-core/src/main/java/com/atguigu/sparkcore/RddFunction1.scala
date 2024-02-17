package com.atguigu.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
 * map算子
 */

object RddFunction1 {

  def main(args: Array[String]): Unit = {

    val rddFunction = new SparkConf().setMaster("local[*]").setAppName("RddFunction")

    val sc = new SparkContext(rddFunction)

    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val newRdd = rdd.map(_ * 2)

    //newRdd.collect().foreach(println)

    val rdd1 = sc.textFile("input/apache.log")

    val newRdd1 = rdd1.map(_.split(" ")(6))

    newRdd1.collect().foreach(println)

    sc.stop()

  }
}
