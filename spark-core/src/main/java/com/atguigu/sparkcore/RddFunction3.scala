package com.atguigu.sparkcore

import org.apache.spark.{SparkConf, SparkContext}


/**
 * mapPartitionsWithIndex算子
 */
object RddFunction3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("RddFunction3")

    val sc   = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4,5,6),3)

    val newRdd = rdd.mapPartitionsWithIndex((index, list) => {
      if (index == 1) {
        list
      }
      else {
       Nil.iterator
      }
      //List((index,list.max)).iterator
    })

    newRdd.collect().foreach(println)

    sc.stop()
  }
}
