import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

object UDAF_Oldversion_class {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("UDAF_Oldversion_class")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val df = spark.read.json("data/user.json")

    //dataFrame.createOrReplaceTempView("t_user")

    //Spark早期版本没有办法将强类型的UDAF函数应用在sql中

    //UDAF是强类型，所以用ds操作
    val ds = df.as[User]

    val udaf = new MyavgageUDAF

    //DSL是面向对象的语法
    ds.select(udaf.toColumn).show

    spark.stop()


  }

  case class User(id: Long,name: String, age: Long)

  case class AvgBuffer(var sum: Long, var cnt: Long)

  //
  class MyavgageUDAF extends Aggregator[User,AvgBuffer,Long]{
    override def zero: AvgBuffer = {
      AvgBuffer(0l,0l)
    }

    override def reduce(b: AvgBuffer, user: User): AvgBuffer = {
      b.sum += user.age
      b.cnt += 1
      b
    }

    override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
      b1.sum += b2.sum
      b1.cnt += b2.cnt
      b1
    }

    override def finish(reduction: AvgBuffer): Long = {
      reduction.sum / reduction.cnt
    }

    override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
