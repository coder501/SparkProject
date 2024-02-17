import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SparkSql {

  def main(args: Array[String]): Unit = {
    //System.setProperty("HADOOP_USER_NAME","atguigu")


    val spark = SparkSession.builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("sql")
      .getOrCreate()

    /*spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS `user_visit_action`(
        |  `date` string,
        |  `user_id` bigint,
        |  `session_id` string,
        |  `page_id` bigint,
        |  `action_time` string,
        |  `search_keyword` string,
        |  `click_category_id` bigint,
        |  `click_product_id` bigint,
        |  `order_category_ids` string,
        |  `order_product_ids` string,
        |  `pay_category_ids` string,
        |  `pay_product_ids` string,
        |  `city_id` bigint)
        |row format delimited fields terminated by '\t';
        |""".stripMargin).show

    spark.sql(
      """
        |load data local inpath 'input1/user_visit_action.txt' into table user_visit_action;
        |""".stripMargin).show*/


    /*spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        |row format delimited fields terminated by '\t';
        |""".stripMargin).show

    spark.sql(
      """
        |load data local inpath 'input1/product_info.txt' into table product_info;
        |""".stripMargin).show*/

   spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,
        |  `area` string)
        |row format delimited fields terminated by '\t';
        |""".stripMargin).show

    spark.sql(
      """
        |load data local inpath 'input1/city_info.txt' into table city_info;
        |""".stripMargin).show()

    //spark.sql("select * from user_visit_action").show()


    //spark.sql("show tables").show

    spark.udf.register("remarkCity",functions.udaf(new MyCityRemark))

    spark.sql(
      """
        |select
        |t3.*
        |from
        |(
        |	select
        |	t2.* ,
        |	rank() over(partition by area order by click_num desc) as rk
        |	from
        |	(
        |		select
        |		area,
        |		product_name,
        |		COUNT(click_product_id) click_num,
        |   remarkCity(city_name) city_remark
        |		from(
        |			select
        |			uv.*,
        |			cin.city_name,
        |			cin.area,
        |			pin.product_name
        |			from
        |			user_visit_action uv
        |			join city_info cin on uv.city_id = cin.city_id
        |			join product_info pin on uv.click_product_id = pin.product_id
        |		) t1
        |		group by area,product_name
        |	) t2
        |) t3
        |where rk <= 3
        |""".stripMargin).show()

    spark.stop()
  }
  case class CityAnalysis(var total: Long, city_map: mutable.Map[String,Long])



  //自定义udf类
  class MyCityRemark extends Aggregator[String,CityAnalysis,String]{
    override def zero: CityAnalysis = {
      CityAnalysis(0l,mutable.Map[String,Long]())
    }

    override def reduce(buffer: CityAnalysis, a: String): CityAnalysis = {
      buffer.total += 1
      val oldCnt = buffer.city_map.getOrElse(a,0l)

      buffer.city_map.update(a,oldCnt + 1)
      buffer
    }

    override def merge(b1: CityAnalysis, b2: CityAnalysis): CityAnalysis = {
      b1.total += b2.total
      b2.city_map.foreach{
        case(city,cnt) =>{
          val oldCnt = b1.city_map.getOrElse(city,0l)
          b1.city_map.update(city,oldCnt + cnt)
        }
      }
      b1
    }

    override def finish(reduction: CityAnalysis): String = {
      val list = ListBuffer[String]()

      val total = reduction.total

      val city_map = reduction.city_map

      val city_list = city_map.toList.sortBy(_._2)(Ordering.Long.reverse)

      val top2 = city_list.take(2)

      var rest = 100l

      top2.foreach{
        case(city,cnt) => {
          val r = cnt * 100l / total
          rest -= r
          list.append(s"${city} ${r}%")
        }
      }

      if(city_list.length > 2){
        list.append(s"其它 ${rest}%")
      }

      list.mkString(",")
    }

    override def bufferEncoder: Encoder[CityAnalysis] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}
