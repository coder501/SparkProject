import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")


    val ssc = new StreamingContext(conf,Seconds(3))

    val lineDS = ssc.socketTextStream("localhost",9999)
    
    val wordlistDS = lineDS.flatMap(_.split(" "))
    
    val wordmap = wordlistDS.map((_,1))
    
    val wordcountDS = wordmap.reduceByKey(_+_)

    wordcountDS.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
