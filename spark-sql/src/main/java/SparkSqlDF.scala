import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSqlDF {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SparkSqlDF")

    val spark  = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._


    val df = spark.read.json("data/user.json")


    df.createOrReplaceTempView("t_user")

    //spark.sql("select * from t_user").show()

    spark.sql("select avg(age) from t_user").show

    df.select("id","name","age").show()

    df.select($"name",$"age" + 1).show()

    df.select($"name",'age + 1).show()

    df.select('name,'age + 1).show()

    df.select('name,'age + 1 as "newage").show()

    df.filter('age > 30).show()

    df.filter("age==40").show()

    println("-------------------------dataset--------------------------")

    val ds = df.as[User]

    ds.filter("age==40").show()

  }

  //从文件中读取数据时，数值类型要用BigInt接收
  case class User(name: String,id: BigInt, age: BigInt)
}
