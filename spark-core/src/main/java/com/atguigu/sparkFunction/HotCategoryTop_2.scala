package com.atguigu.sparkFunction

import org.apache.spark.{SparkConf, SparkContext}

object HotCategoryTop_2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("HotCategoryTop_2")

    val sc = new SparkContext(conf)


    val data = sc.textFile("input/user_visit_action.txt")

    val clickDatas = data.filter {
      line => {
        val lines = line.split("_")
        lines(6) != "-1"
      }
    }.map {
      line => {
        val lines = line.split("_")
        (lines(6), 1)
      }
    }.reduceByKey(_ + _)


    val orderDatas = data.filter {
      line => {
        val lines = line.split("_")
        lines(8) != null
      }
    }.flatMap {
      line => {
        val lines = line.split("_")
        val categorys = lines(8).split(",")
        categorys.map(category => (category, 1))
      }
    }.reduceByKey(_ + _)

    val payDatas = data.filter {
      line => {
        val lines = line.split("_")
        lines(10) != null
      }
    }.flatMap {
      line => {
        val lines = line.split("_")
        val categorys = lines(10).split(",")
        categorys.map(category => (category, 1))
      }
    }.reduceByKey(_ + _)

    val clickData = clickDatas.map {
      case (category, count) => (category, (count, 0, 0))
    }


    val orderData = orderDatas.map {
      case (category, count) => (category, (0, count, 0))
    }


    val payData = payDatas.map {
      case (category, count) => (category, (0, 0, count))
    }

    val totalData = clickData.union(orderData).union(payData).reduceByKey {
      case (t1, t2) => ((t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3))
    }

    val top10 = totalData.sortBy(_._2,false).take(10)
    //val top10 = totalData.top(10)

    top10.foreach(println)


  }
}
