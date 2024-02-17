import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwoDStreamJoin {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("TwoDStreamJoin")

    val ssc = new StreamingContext(conf,Seconds(3000))

    val socketDStream1 = ssc.socketTextStream("local",9999)
    val socketDStream2 = ssc.socketTextStream("localhost",8888)

    socketDStream1.map((_,9999)).join(socketDStream1.map((_,8888)))

    //val DSmap = socketDStream1.map((_,9999))

    ssc.start()
    ssc.awaitTermination()

  }
}
