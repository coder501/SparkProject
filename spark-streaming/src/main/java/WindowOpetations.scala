import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowOpetations {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("DStreamTransform")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val inputDStream = ssc.socketTextStream("localhost",9999)


    val wordToOne =
      inputDStream
          .map((_,1))
          .window(Seconds(6),Seconds(3))
          .reduceByKey(_+_)
          .print()


    ssc.start()

    ssc.awaitTermination()
  }
}
