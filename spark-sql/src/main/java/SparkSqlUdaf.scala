import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

object SparkSqlUdaf {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SparkSqlUdaf")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val dataFrame = spark.read.json("data/user.json")

    dataFrame.createOrReplaceTempView("t_user")


    spark.udf.register("avgAge",functions.udaf(new MyAvgAgeUDAF()))


    spark.sql("select avgAge(age) from t_user").show()


    spark.stop()
  }

  case class AvgBuffer(var sum: Long, var cnt: Long)



  //org.apache.spark.sql.expressions.Aggregator
  class MyAvgAgeUDAF extends Aggregator[Long,AvgBuffer,Long]{
    override def zero: AvgBuffer = {
      AvgBuffer(0l,0l)
    }

    override def reduce(buffer: AvgBuffer, age: Long): AvgBuffer = {
      buffer.sum += age
      buffer.cnt += 1
      buffer
    }

    override def merge(buffer1: AvgBuffer, buffer2: AvgBuffer): AvgBuffer = {
      buffer1.sum += buffer2.sum
      buffer1.cnt += buffer2.cnt
      buffer1
    }

    override def finish(reduction: AvgBuffer): Long = {
      reduction.sum / reduction.cnt
    }

    override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
