import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RDDStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("RDDStream")

    val ssc = new StreamingContext(conf,Seconds(3))


    val rddQueue = new mutable.Queue[RDD[String]]()


    val inputStream = ssc.queueStream(rddQueue,oneAtATime = false)

    val wordToOneDS = inputStream.flatMap(_.split(" ")).map((_,1))


    val reduceDStream = wordToOneDS.reduceByKey(_+_)

    reduceDStream.print()

    ssc.start()

    for(i <- 1 to 5){
      rddQueue.enqueue(ssc.sparkContext.makeRDD(List("a","a","a")))
      //ssc.sparkContext.makeRDD(List("a","a","a"))
      Thread.sleep(2000)
    }


    ssc.awaitTermination()

  }
}
