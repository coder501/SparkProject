import java.io.{BufferedReader, InputStreamReader}
import java.net.{ServerSocket, Socket}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UDFreceiver {

  def main(args: Array[String]): Unit = {

    //
    val conf = new SparkConf().setMaster("local[*]").setAppName("UDFreceiver")


    val ssc = new StreamingContext(conf,Seconds(3))

    val ds = ssc.receiverStream(new MyReceiver("localhost",9999))

    //val words = ds.flatMap(_.split(" "))
    
    val wordToOne = ds.map((_,1))
    
    val wordCountStream = wordToOne.reduceByKey(_+_)


    wordCountStream.print()

    ssc.start()

    ssc.awaitTermination()

  }

  class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
    override def onStart(): Unit = {
        new Thread("Socket Server"){
          override def run(): Unit = {
            receiver()
          }
        }.start()
    }

    def receiver(): Unit ={
      val socket = new Socket(host,port)

      var input: String = null

      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))
      
      input = reader.readLine()

      while(!isStopped() &&  input != null){
        store(input)
        input = reader.readLine()
      }

      reader.close()
      socket.close()

      restart("restart")

    }

    override def onStop(): Unit = {

    }
  }
}
