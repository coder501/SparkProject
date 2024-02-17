import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSqlEnv {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SparkSqlEnv")


    val spark = SparkSession.builder().config(conf).getOrCreate()



    spark.stop()
  }
}
