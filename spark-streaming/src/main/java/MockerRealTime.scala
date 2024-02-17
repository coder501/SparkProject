import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object MockerRealTime {

  def generateMockData(): Array[String] = {
    val array: ArrayBuffer[String] = ArrayBuffer[String]()

    val CityRandomOpt = RandomOptions(Ranopt(CityInfo(1, "北京", "华北"), 30),
      Ranopt(CityInfo(2, "上海", "华东"), 30),
      Ranopt(CityInfo(3, "广州", "华南"), 10),
      Ranopt(CityInfo(4, "深圳", "华南"), 20),
      Ranopt(CityInfo(5, "天津", "华北"), 10))


    val random = new Random()

    //模拟实时数据
    //timestamp province city userid adid
    for(i <- 0 to 50){

      val timestamp = System.currentTimeMillis()
      val city_info = CityRandomOpt.getRandomOpt
      val city = city_info.City_name
      val area = city_info.area
      val userID = 1 + random.nextInt(6)
       val adId = 1 + random.nextInt(6)

      array += timestamp + " " + area + " " + city + " " + userID + " " + adId
    }
    array.toArray
  }


  def createKafkaProduce(broker: String): KafkaProducer[String,String] = {
    val prop = new Properties()
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,broker)
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String,String](prop)
  }



  def main(args: Array[String]): Unit = {

    val config = PropertiesUtil.load("config.properties")

    val broker = config.getProperty("kafka.broker.list")

    val topic = "sparkstreaming"
    val kafkaProducer = createKafkaProduce(broker)

    while(true){

      for(line <- generateMockData()){
        kafkaProducer.send(new ProducerRecord[String,String](topic,line))
        println(line)
      }

      Thread.sleep(5000)
    }

  }
}
