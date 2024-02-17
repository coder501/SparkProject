import scala.collection.mutable.ListBuffer
import scala.util.Random

case class CityInfo(City_id: Long, City_name: String, area: String)

case class Ranopt[T](value: T, weight: Int)

object RandomOptions {

  def apply[T](opts: Ranopt[T]*): RandomOptions[T] = {
    val randomOptions = new RandomOptions[T]()

    for(opt <- opts){
      randomOptions.totalweight += opt.weight
      for(i <- 1 to opt.weight){
        randomOptions.optsBuffer += opt.value
      }
    }

    randomOptions
  }
}

class RandomOptions[T](opts: Ranopt[T]*){
  var totalweight = 0
  var optsBuffer = new ListBuffer[T]

  def getRandomOpt: T = {
    val randomNum = new Random().nextInt(totalweight)
    optsBuffer(randomNum)
  }
}
