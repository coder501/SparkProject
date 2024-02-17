package com.atguigu.sparkcore

import org.apache.spark.{SparkConf, SparkContext}



object RddPartition {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")


    val context = new SparkContext(conf)

    val dataRdd = context.parallelize(List(1,2,3,4),2)

    dataRdd.saveAsTextFile("output")

  }
}
