package com.atguigu.sparkFunction

import org.apache.spark.{SparkConf, SparkContext}

object WordCount4 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount4")

    val sc = new SparkContext(sparkConf)

    val words = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3), ("d", 4),
      ("b", 8), ("a", 5), ("c", 3), ("d", 2)
    ))

    //1.groupby
    val rdd1 = words.groupBy(_._1).mapValues(_.map(_._2).sum)

    rdd1.collect().foreach(println)

  }
}
