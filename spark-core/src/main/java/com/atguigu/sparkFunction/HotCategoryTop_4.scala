package com.atguigu.sparkFunction

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object HotCategoryTop_4 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("HotCategoryTop_4")

    val sc = new SparkContext(conf)

    val data = sc.textFile("input/user_visit_action.txt")

    val wordAcc = new WordCountAccumulate()

    sc.register(wordAcc,"wordCount")



    data.filter {
      line => {
        val lines = line.split("_")
        lines(6) != "-1"
      }
    }.foreach {
      line => {
        val lines = line.split("_")
        wordAcc.add((lines(6),(1,0,0)))
      }
    }


    data.filter {
      line => {
        val lines = line.split("_")
        lines(8) != "null"
      }
    }.foreach{
      line => {
        val lines = line.split("_")
        val categorys = lines(8).split(",")
        categorys.foreach{
          category => {
            wordAcc.add( (category,(0,1,0)) )
          }
        }

       }
      }



    data.filter {
      line => {
        val lines = line.split("_")
        lines(10) != "null"
      }
    }.foreach {
      line => {
        val lines = line.split("_")
        val categorys = lines(10).split(",")
        categorys.foreach{
          category => {
            wordAcc.add( (category,(0,0,1)) )
          }
        }
      }
    }

    wordAcc.value.toList.sortBy(_._2)(Ordering.Tuple3[Int,Int,Int].reverse).take(10).foreach(println)

    sc.stop()
  }

  class WordCountAccumulate extends AccumulatorV2[(String,(Int,Int,Int)),mutable.Map[String,(Int,Int,Int)]]{

    private val map = mutable.Map[String,(Int,Int,Int)]()

    override def isZero: Boolean = map.isEmpty

    override def copy(): AccumulatorV2[(String, (Int, Int, Int)), mutable.Map[String, (Int, Int, Int)]] = new WordCountAccumulate()

    override def reset(): Unit = map.clear()

    override def add(v: (String, (Int, Int, Int))): Unit = {
      val oldCount = map.getOrElse(v._1,(0,0,0))
      map.update(v._1,(v._2._1 + oldCount._1,v._2._2 + oldCount._2,v._2._3+oldCount._3))
    }

    override def merge(other: AccumulatorV2[(String, (Int, Int, Int)), mutable.Map[String, (Int, Int, Int)]]): Unit = {
      val map2 = other.value

      map2.foldLeft(map){
        (innerMap,kv) => {

          val oldValue =  innerMap.getOrElse(kv._1,(0,0,0))
          val newValue = kv._2
          innerMap.update(kv._1,(oldValue._1 + newValue._1,oldValue._2 + newValue._2,oldValue._3+newValue._3))

          innerMap
        }

      }

    }

    override def value: mutable.Map[String, (Int, Int, Int)] = map
  }
}


