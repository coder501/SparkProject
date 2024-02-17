import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamUpdateStateByKey {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[2]").setAppName("DStreamUpdateStateByKey")

    val ssc = new StreamingContext(conf,Seconds(3))

    ssc.checkpoint("op")

    val socketDStream = ssc.socketTextStream("localhost",9999)

    // 有状态操作就意味着要保留中间数据（每一个采集周期的数据）
    // reduceByKey方法是无状态的，所以不能使用
    // The checkpoint directory has not been set. Please set it by StreamingContext.checkpoint().
    socketDStream.flatMap(_.split(" "))
      .map((_,1))
      .updateStateByKey(
        //seq是相同的键的值的集合，buffer是一个缓冲区，用来记录状态，Option用来解决空指针异常
        (seq: Seq[Int], buffer: Option[Int]) => {
            val newCnt = seq.sum + buffer.getOrElse(0)
            Option(newCnt)
        }
      ).print()

    ssc.start()

    ssc.awaitTermination()


  }

}
