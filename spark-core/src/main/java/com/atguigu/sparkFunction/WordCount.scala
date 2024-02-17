package com.atguigu.sparkFunction

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")

    val sc = new SparkContext(sparkConf)

    val wordlist = sc.makeRDD(List(
      ("a", 1), ("b", 4), ("a", 6),
      ("b", 2), ("b", 3), ("a", 8)
    ), 2)

    //1.groupBy
    val groupRdd = wordlist.groupBy(t => t._1)

    val sumRdd1 = groupRdd.mapValues(_.map(_._2).sum)


    sumRdd1.collect().foreach(println)

    //2.groupByKey
    val groupByKeyRdd = wordlist.groupByKey()

    val sumRdd2 = groupByKeyRdd.mapValues(_.map(word => word).sum)

    sumRdd2.collect().foreach(println)


    //3.reduceByKey
    val reduceBykeyRdd = wordlist.reduceByKey(_+_)

    reduceBykeyRdd.collect().foreach(println)

    //4.aggregateByKey

    val aggregateByKeyRdd = wordlist.aggregateByKey(0)(
      (x,y) => x + y,
      (x,y) => x + y
    )

    aggregateByKeyRdd.collect().foreach(println)

    //5.foldByKey
    val foldByKeyRdd = wordlist.foldByKey(0)(
      (x, y) => x + y
    )
    foldByKeyRdd.collect().foreach(println)

    //6.combineByKey
    val combineByKey = wordlist.combineByKey(
      v => v,
      (t: Int, v) => t + v,
      (t1: Int, t2: Int) => t1 + t2
    )
    combineByKey.collect().foreach(println)

    //7.countByKey  action算子不用调collect()方法
    val countFlatRdd = wordlist.flatMap {
      case (word: String, count: Int) => {
        val num = count
        val list = new ListBuffer[(String,Int)]
        for (i <- 1 to num) {
          val t = (word, num)
          list.append(t)
        }
        list
          //.toIterator
      }
    }

    val countByKeyRdd = countFlatRdd.countByKey()

    countByKeyRdd.foreach(println)

    //8.countByValue

    val countValueFlat = wordlist.flatMap {
      case (word: String, count: Int) => {
        val num = count
        val list = new ListBuffer[(String, Int)]
        for (i <- 1 to num) {
          list.append((word,1))
        }
        list
      }
    }
    val countByValueRdd = countValueFlat.countByValue()

    countByValueRdd.foreach(println)

  }
}
