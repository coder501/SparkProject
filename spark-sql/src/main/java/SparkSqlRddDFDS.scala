import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSqlRddDFDS {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local").setAppName("SparkSqlRddDFDS")


    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    //val dataFrame = spark.read.json("data/user.json")

    val rdd = spark.sparkContext.makeRDD(List((1,"zhangsan",30),(2,"lisi",40)))


    //rdd ->  df
    //df只关注结构，ds关注类型

    val dataFrame = rdd.toDF("id","name","age")

    //df -> ds

    val ds = dataFrame.as[User]

    //ds -> df

    val df = ds.toDF()

    //df -> rdd
    val rdd1 = df.rdd

    //rdd -> ds

    val userRdd = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }

    val ds1 = userRdd.toDS()

    val rdd2 = ds1.rdd

  }

  case class User(id: Int, name: String, age: Int)
}
