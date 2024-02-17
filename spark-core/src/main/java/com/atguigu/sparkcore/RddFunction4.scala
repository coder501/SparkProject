package com.atguigu.sparkcore

import org.apache.spark.{SparkConf, SparkContext}


/**
 * flatMap算子
 */
object RddFunction4 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local").setAppName("RddFunction4")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List("hello world","hello spark"))

    //f:String => TraversableOnce[U]
    val newRdd = rdd.flatMap(list => list.split(" "))


    val rdd1 = sc.makeRDD(List(List(1,2),List(3,4)))

    //f:List[Int] => TraversableOnce[U]
    val newRdd1 = rdd1.flatMap(list => list)

    sc.stop()
  }
}
