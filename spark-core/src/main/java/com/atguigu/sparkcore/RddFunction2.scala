package com.atguigu.sparkcore

import org.apache.spark.{SparkConf, SparkContext}


/**
 * mapPartitions算子
 */
object RddFunction2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("RddFunction2")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)


    //输入是一个可迭代的集合，输出也是一个可迭代的集合
    //f(Iterator[Int] => Iterator[U],preservesPartitioning: Boolean = false)
    val newRdd = rdd.mapPartitions(list => {
      List(list.max).iterator
    })

    //获取分区的同时获取分区索引
    val newRdd2 = rdd.mapPartitionsWithIndex((index, list) => {
      List((index, list.max)).iterator
    })


    newRdd2.collect().foreach(println)

    newRdd.collect().foreach(println)

    sc.stop()
  }
}
