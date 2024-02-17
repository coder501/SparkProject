package com.atguigu.sparkcore

import org.apache.spark.{SparkConf, SparkContext}


object WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("WordCount");


    val sc = new SparkContext(sparkConf);

    val value = sc.textFile("input/word.txt")

    val value1 = value.flatMap(_.split(" "))

    val value2 = value1.map((_,1))

    /*val value3 = value2.groupBy(_._1)

    val value4 = value3.mapValues(_.map(_._2).sum)*/
    val value4 = value2.reduceByKey(_+_)

    value4.collect().foreach(println)




    sc.stop()
  }
}
