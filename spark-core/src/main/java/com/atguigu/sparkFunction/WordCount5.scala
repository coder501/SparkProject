package com.atguigu.sparkFunction

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object WordCount5 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount5")

    val sc = new SparkContext(conf)

    val words = sc.makeRDD(List("Hello Spark","Hello Flink","Hello Kafka"))

    //1.groupby


    val rdd1 = words.flatMap(_.split(" ").groupBy(word => word)).mapValues(_.size)

    rdd1.collect().foreach(println)

    //2.groupbyKey

    val rdd2 = words.flatMap(_.split(" ")).map((_,1)).groupByKey().mapValues(_.sum)

    rdd2.collect().foreach(println)

    //3.reduceByKey

    val rdd3 = words.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    rdd3.collect().foreach(println)

    //4.aggregateByKey

    val rdd4 = words.flatMap(_.split(" ")).map((_, 1)).aggregateByKey(0)(
      (x, y) => x + y,
      (x, y) => x + y
    )
    rdd4.collect().foreach(println)


    //5.foldByKey

    val rdd5 = words.flatMap(_.split(" ")).map((_, 1)).foldByKey(0)(
      (x, y) => x + y
    )
    rdd5.collect().foreach(println)

    //6.combineByKey
    val rdd6 = words.flatMap(_.split(" ")).map((_, 1)).combineByKey(
      v => v,
      (t: Int, v) => t + v,
      (t1: Int, t2: Int) => t1 + t2
    )
    rdd6.collect().foreach(println)

    //7.countByKey
    val rdd7 = words.flatMap(_.split(" ")).map((_,1)).countByKey()

    rdd7.foreach(println)


    //8.countByValue

    val rdd8 = words.flatMap(_.split(" ")).map((_,1)).countByValue().map(t => (t._1._1,t._2))



    rdd8.foreach(println)

    //9.aggregate

    val rdd9 = words.flatMap(_.split(" ")).aggregate(mutable.Map[String,Int]())(
      (map,s) =>{
        map(s) = map.getOrElse(s,0) + 1
        map
      },
      (map1,map2) => {
        map1.foldLeft(map2){
          (innerMap,kv) => {
            innerMap(kv._1) = innerMap.getOrElse(kv._1,0) + kv._2
            innerMap
          }
        }
      }


    )


    rdd9.foreach(println)

    //10.fold

    val rdd10 = words.flatMap(_.split(" "))
      .map(s => mutable.Map((s,1)))
      .fold(mutable.Map[String,Int]())(
        (map1,map2) => {
          map1.foldLeft(map2){
            (innerMap,kv) => {
              innerMap(kv._1) = innerMap.getOrElse(kv._1,0) + kv._2
            }
              innerMap
          }
        }
      )

    rdd10.foreach(println)

  }
}
