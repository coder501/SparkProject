package com.atguigu.sparkFunction

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object HotCategoryTop_3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("HotCategoryTop_3")

    val sparkContext = new SparkContext(conf)

    val data = sparkContext.textFile("input/user_visit_action.txt")

    val clickDatas = data.filter {
      line => {
        val lines = line.split("_")
        lines(6) != "-1"
      }
    }.map {
      line => {
        val lines = line.split("_")
        (lines(6), (1, 0, 0))
      }
    }


    val orderDatas = data.filter {
      line => {
        val lines = line.split("_")
        lines(8) != null
      }
    }.flatMap {
      line => {
        val lines = line.split("_")
        val categorys = lines(8).split(",")
        categorys.map(category => (category, (0, 1, 0)))
      }
    }

    val payDatas = data.filter {
      line => {
        val lines = line.split("_")
        lines(10) != null
      }
    }.flatMap {
      line => {
        val lines = line.split("_")
        val categorys = lines(10).split(",")
        categorys.map(category => (category, (0, 0, 1)))
      }
    }


    val totalDatas = clickDatas.union(orderDatas).union(payDatas).reduceByKey((t1,t2) => (t1._1+t2._1,t1._2+t2._2,t1._3+t2._3))

    val top10 = totalDatas.sortBy(_._2,false).take(10)


    top10.foreach(println)
  }
}


