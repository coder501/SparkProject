package com.atguigu.sparkFunction

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable





object WordCount2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("WordCount2")

    val sparkContext = new SparkContext(conf)

    val data = sparkContext.makeRDD(List("hello spark","hello kafka","hello flink"))


    //1.groupBy    SingleValue

    val flatRdd = data.flatMap(_.split(" "))

    val result1 = flatRdd.groupBy(word => word).mapValues(_.size)

    result1.collect().foreach(println)

    println("-----------------groupby--------------------")

    //2.groupByKey   transformKV

    val groupRdd = data.flatMap(_.split(" ")).map((_,1)).groupByKey()

    val result2 = groupRdd.mapValues(_.sum)

    result2.collect().foreach(println)

    println("-----------------groupbykey--------------------")

    //3.reduceByKey    transformKV
    val reslut3 = data.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)


    reslut3.collect().foreach(println)

    println("-----------------reducebykey--------------------")


    //4.aggregateByKey      transformKV
    val result4 = data.flatMap(_.split(" ")).map((_, 1)).aggregateByKey(0)(
      (x, y) => x + y,
      (x, y) => x + y
    )
    result4.collect().foreach(println)

    println("-----------------aggregateByKey--------------------")

    //5.foldByKey        transformKV

    val result5 = data.flatMap(_.split(" ")).map((_, 1)).foldByKey(0)(
      (x, y) => x + y
    )
    result5.collect().foreach(println)

    println("-----------------foldByKey--------------------")

    //6.combineByKey   transformKV

    val result6 = data.flatMap(_.split(" ")).map((_, 1)).combineByKey(
      v => v,
      (t: Int, v) => (t + v),
      (t1: Int, t2: Int) => (t1 + t2)
    )
    result6.collect().foreach(println)
    println("-----------------combineByKey--------------------")
    //7.countByKey     action
   val result7 = data.flatMap(_.split(" ")).map((_,1)).countByKey()

    result7.foreach(println)

    println("-----------------countByKey--------------------")

    //8.countByValue   action
    val result8 = data.flatMap(_.split(" ")).map((_,1)).countByValue().map(m =>(m._1._1,m._2))

    result8.foreach(println)

    println("-----------------countByValue--------------------")


    //9.aggregate    singleValue
   /* val words = data.flatMap(_.split(" "))


    words.aggregate(mutable.Map)(
      (map,s) => {
        map(s) = map.getOrElse(s,0)
      }
    )*/

    val words = data.flatMap(_.split(" "))

    words.aggregate(mutable.Map[String,Int]())(
      (map,s) => {
        map(s) = map.getOrElse(s,0) + 1
        map
      },
      (map1,map2) => {
        map1.foldLeft(map2){
          (innerMap,kv) =>{
            innerMap(kv._1) = innerMap.getOrElse(kv._1,0) + kv._2
            innerMap
          }
        }
      }
    ).foreach(println)

    println("----------------aggregate------------------------")

    //10.fold    singleValue
    val words1 = data.flatMap(_.split(" "))

    words1.map(s => mutable.Map((s,1)))
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

    println("------------------fold-------------------------")


    //11.reduce
    data.flatMap(_.split(" ")).map(s => mutable.Map((s,1))).reduce(
      (map1,map2) => {

        map1.foldLeft(map2){
          (innerMap,kv) => {
            innerMap(kv._1) = innerMap.getOrElse(kv._1,0) + kv._2
          }
            innerMap
        }

      }
    ).foreach(println)





  }
}
