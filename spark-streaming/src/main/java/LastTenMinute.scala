import java.io.PrintWriter
import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable.ListBuffer

object LastTenMinute {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")


    val ssc = new StreamingContext(conf,Seconds(5))

    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("sparkstreaming"), kafkaPara))


    //kafka中的数据都是以k-v对出现，但是k-v对(ProducerRecord)不能序列化,只需要将值取出来
    val kafkaDatas = kafkaDStream.map(_.value())

    kafkaDatas.map{
      line => {
        val datas = line.split(" ")
        val ts = datas(0).toLong

        (ts / 10000 * 10000,1)
      }
    }.window(Seconds(60),Seconds(10))
        .reduceByKey(_+_).foreachRDD(
      rdd => {
        val datas = rdd.collect()

        val listBuffer = ListBuffer[String]()

        datas.sortBy(_._1).foreach{
          case(ts, cnt) => {
            val time = new SimpleDateFormat("mm:ss").format(new java.util.Date(ts))
            listBuffer.append(
              s"""
                |{"xtime":"${time}", "yval":"${cnt}"}
                |""".stripMargin)

          }
        }

        val out = new PrintWriter("D:\\bigdata\\ideaworkplace\\SparkProject\\adclick\\adclick.json")

        out.write("["+ listBuffer.mkString(",") + "]")
        out.flush()
        out.close()

      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
