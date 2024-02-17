import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

object SparkSqlUdf2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SparkSqlUdf2")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val dataFrame = spark.read.json("data/user.json")

    dataFrame.createOrReplaceTempView("t_user")

    spark.udf.register("mySum",functions.udaf(new MySumAggregator))

    spark.sql("select mySum(age) from t_user").show()

    spark.stop()
  }
  case class SumBuffer(var sum: Long)

  class MySumAggregator extends Aggregator[Long,SumBuffer,Long]{
    override def zero: SumBuffer = {
      SumBuffer(0l)
    }

    override def reduce(buffer: SumBuffer, age: Long): SumBuffer = {
      buffer.sum += age
      buffer
    }

    override def merge(b1: SumBuffer, b2: SumBuffer): SumBuffer = {
      b1.sum += b2.sum
      b1
    }

    override def finish(reduction: SumBuffer): Long = {
      reduction.sum
    }

    override def bufferEncoder: Encoder[SumBuffer] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
