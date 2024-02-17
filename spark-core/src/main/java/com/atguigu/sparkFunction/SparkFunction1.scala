package com.atguigu.sparkFunction

import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.util.collection.{AppendOnlyMap, ExternalAppendOnlyMap}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.convert.Wrappers
import scala.collection.generic.IterableForwarder
import scala.collection.{AbstractIterable, IterableProxy, IterableViewLike, immutable, mutable}
import scala.reflect.io.AbstractFile
import scala.xml.MetaData

object SparkFunction1 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local").setAppName("SparkFunction1")

    val sparkContext = new SparkContext(conf)

    val rdd = sparkContext.textFile("input/agent.log")

    val  filterRdd = rdd.map(line => {
          val lines = line.split(" ")
          (lines(1),lines(4))
    })

  // val grouprdd = filterRdd.groupBy(_._1)

    val groupRdd = filterRdd.groupByKey()


    val resRdd = groupRdd.mapValues(
      wordlist => {
        val adGroupMap = wordlist.map((_, 1)).groupBy(_._1)

        val adTocountMap = adGroupMap.mapValues(_.size)

        adTocountMap.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
    resRdd.collect().foreach(println)

    sparkContext.stop()


    //val mapRdd = grouprdd.mapValues(t => t.map(t=>(t._2,1)))


  }
}
