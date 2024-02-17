import java.io.PrintWriter
import java.sql.DriverManager
import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable.ListBuffer

object BlockList {


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

    val config = PropertiesUtil.load("config.properties")

    val driver = config.getProperty("jdbc.driver")
    val url = config.getProperty("jdbc.url")
    val user = config.getProperty("jdbc.user")
    val password = config.getProperty("jdbc.password")

    Class.forName(driver)

    val connection = DriverManager.getConnection(url,user,password)


    val statement = connection.prepareStatement(
      """
        |select userid from  black_list
        |""".stripMargin)



    //val list = ListBuffer[String]()


    /*kafkaDatas.filter{
      line => {
        val datas = line.split(" ")
        val userid = datas(0)
        !list.contains(userid)
      }
    }*/


    kafkaDatas.transform{
      rdd => {
        //周期性执行
        val resultSet = statement.executeQuery()
        val list = ListBuffer[String]()
        while(resultSet.next()){
          val userid = resultSet.getString("userid")
          list.append(userid)
        }
        rdd.filter{
          line => {
            val datas = line.split(" ")
            val userid = datas(0)
            !list.contains(userid)
          }
        }
      }
    }



    kafkaDatas.map{
      line => {
        val datas = line.split(" ")
        val ts = datas(0).toLong

        (ts / 10000 * 10000,1)
      }
    }.window(Seconds(60),Seconds(10))
      .reduceByKey(_+_)

    ssc.start()
    ssc.awaitTermination()
  }
}
