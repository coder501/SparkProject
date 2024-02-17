package com.atguigu.sparkFunction

import org.apache.spark.{SparkConf, SparkContext}

object SparkFunction2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SparkFunction2")

    val sparkContext = new SparkContext(conf)

    val rdd = sparkContext.textFile("input/agent.log")


    val etlRdd = rdd.map(line => {
      val lines = line.split(" ")

      ((lines(1), lines(4)), 1)
    })

    val reduceRdd = etlRdd.reduceByKey((x,y) => x + y)

    /*val prvRdd = reduceRdd.map(
      line => line match {
        case ((prv, ad), sum) => (prv, (ad, sum))
      }
    )*/

    val proRdd = reduceRdd.map {
      case ((prv, ad), sum) => (prv, (ad, sum))
    }

    val groupRdd = proRdd.groupByKey()


    //val r = groupRdd.mapValues(_.map(_._2).sum)


      /*groupRdd.mapValues(
        iter => {
          iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
        }
      ).collect().foreach(println)*/

    val r = groupRdd.mapValues(_.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))


    r.collect().foreach(println)


  }
}
