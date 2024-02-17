package com.atguigu.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

object RddPartition2 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")


    val context = new SparkContext(sparkConf)

    val dataRdd = context.textFile("input/word.txt")

    dataRdd.saveAsTextFile("output")


  }
}
