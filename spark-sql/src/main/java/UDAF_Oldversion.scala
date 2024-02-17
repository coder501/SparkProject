import SparkSqlUdaf.MyAvgAgeUDAF
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

object UDAF_Oldversion {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("UDAF_Oldversion")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val dataFrame = spark.read.json("data/user.json")

    dataFrame.createOrReplaceTempView("t_user")


    spark.udf.register("avgAge",new MyavgAgeUDAF)


    spark.sql("select avgAge(age) from t_user").show()

    spark.stop()

  }

  class MyavgAgeUDAF extends  UserDefinedAggregateFunction{
    override def inputSchema: StructType = {
      StructType{
        Array{
          StructField("age",LongType)
        }
      }
    }

    override def bufferSchema: StructType = {
      StructType{
        Array{
          StructField("sum",LongType)
          StructField("cnt",LongType)
        }
      }
    }

    override def dataType: DataType = {
      LongType
    }

    override def deterministic: Boolean = true


    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0,0l)
      buffer.update(1,0l)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0,buffer.getLong(0) + input.getLong(0))
      buffer.update(1,buffer.getLong(1)  + 1)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0,buffer1.getLong(0)+buffer2.getLong(0))
      buffer1.update(1,buffer1.getLong(1)+buffer2.getLong(1))
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }


  }
}
