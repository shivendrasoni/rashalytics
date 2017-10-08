package org.vikings.hackathons.application

import org.vikings.hackathons.utils.Driver
import com.redis._
import org.vikings.hackathons.streaming.MetricsCounter


class CacheLayer(client: RedisClient) extends Serializable {

  def setMetricsCounters(driverId: Int, counters: MetricsCounter): Unit = {
    client.hmset(driverId,
      Map(
        "cntHA" -> counters.cntHA,
        "cntHD" -> counters.cntHD,
        "cntSLT" -> counters.cntSLT,
        "cntSRT" -> counters.cntSRT,
        "cntSLC" -> counters.cntSLC,
        "total" -> counters.total
      )
    )
  }

  def getMetricsCounters(driverId: Int): Option[MetricsCounter] = {
    val exist = client.exists(driverId)
    if (exist) {
      val prevValMapOption = client.hmget(driverId, "cntHA", "cntHD", "cntSLT", "cntSRT", "cntSLC", "total")
      val prevValMap = prevValMapOption.get
      Some(MetricsCounter(
        prevValMap("cntHA").toInt,
        prevValMap("cntHD").toInt,
        prevValMap("cntSLT").toInt,
        prevValMap("cntSRT").toInt,
        prevValMap("cntSLC").toInt,
        prevValMap("total").toInt
      ))
    }
    else
      None
  }

}

object CacheLayer {
  def main(args: Array[String]): Unit = {
    val r = new RedisClient("localhost", 32769)
    r.set(123, "123.0")
    println(r.get("124").get.toDouble + 12.3)
  }
}