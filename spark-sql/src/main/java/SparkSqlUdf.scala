import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSqlUdf {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SparkSqlUdf")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val dataFrame = spark.read.json("data/user.json")

    dataFrame.createOrReplaceTempView("t_user")

    //向udf中注册自定义函数名和函数的功能
    spark.udf.register("prefixName",(name: String) => {
      "Name:" + name
    })

    spark.sql("select id,prefixName(name),age from t_user").show()


  }
}
