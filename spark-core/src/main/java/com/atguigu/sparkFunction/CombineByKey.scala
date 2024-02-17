package com.atguigu.sparkFunction

import org.apache.spark.{SparkConf, SparkContext}

object CombineByKey {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("CombineByKey")

    val sparkContext = new SparkContext(conf)

    val rdd = sparkContext.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)

    rdd.combineByKey(
      v=>(v,1),
      (t:(Int,Int),v) => (t._1 + v,t._2 +1),
      (t1:(Int,Int),t2:(Int,Int)) => (t1._1 + t2._1,t1._2 + t2._2)
    )

    val newRdd = rdd.combineByKey(
      v => v,
      (t: Int, v) => (t + v),
      (t1: Int, t2: Int) => (t1 + t2)
    )

    newRdd.collect().foreach(println)

    sparkContext.stop()

  }
}
