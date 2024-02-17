package com.atguigu.sparkFunction

import org.apache.spark.{SparkConf, SparkContext}

object  HotCategoryTop {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("HotCategoryTop")

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
        ((lines(6), 1))
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
        categorys.map(s => (s, 1))
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
        categorys.map(s => (s, 1))
      }
    }.reduceByKey(_ + _)

    val cogroupRdd =
      clickDatas.cogroup(orderDatas,payDatas)

    val resultRdd = cogroupRdd.mapValues(t => (t._1.toIterator.map(word => word).sum,t._2.toIterator.map(word => word).sum,t._3.toIterator.map(word => word).sum))


    val resultRdd1 = cogroupRdd.mapValues {
      case (iterable1, iterable2, iterable3) => {
        var clickCnt = 0
        var orderCnt = 0
        var payCnt = 0

        val itertor1 = iterable1.toIterator
        if (itertor1.hasNext) {
          clickCnt = itertor1.next()
        }

        val itertor2 = iterable2.toIterator
        if (itertor2.hasNext) {
          orderCnt = itertor2.next()
        }

        val itertor3 = iterable3.toIterator

        if (itertor3.hasNext) {
          payCnt = itertor3.next()
        }

        (clickCnt, orderCnt, payCnt)
      }
    }


    val top10 = resultRdd.sortBy(_._2,false).take(10)


    top10.foreach(println)

  }
}
