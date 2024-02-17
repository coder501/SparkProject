import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamTransformRdd {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("DStreamTransform")

    val ssc = new StreamingContext(sparkConf,Seconds(10))

    val inputDStream = ssc.socketTextStream("localhost",9999)


    val wordToOne = inputDStream.map((_,1))

    val wordCountDStream = wordToOne.reduceByKey(_+_)

    val wordCountSort = wordCountDStream.transform {
      rdd => {
        rdd.sortBy(_._2)
      }
    }
    wordCountSort.print()

    ssc.start()

    ssc.awaitTermination()

  }
}
