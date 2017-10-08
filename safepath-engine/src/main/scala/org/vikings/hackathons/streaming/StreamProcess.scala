package org.vikings.hackathons.streaming

import com.redis.RedisClient
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.vikings.hackathons.application
import org.vikings.hackathons.application.{CacheLayer, Metrics, RawData}
import org.vikings.hackathons.db.DBClient
import org.vikings.hackathons.models.ScoreGenerator
import org.vikings.hackathons.utils.{Constants, Driver, IncidentTypes}

import scala.collection.mutable
import scala.collection.mutable.Queue


class StreamProcess(conf: SparkConf, ssc: StreamingContext) extends Serializable {

  def readStream(fileName: String): DStream[String] = {

    val inRDD: DStream[String] = ssc.textFileStream(fileName)
    //    val rddQueue: mutable.Queue[RDD[String]] = mutable.Queue()
    //    rddQueue += inRDD
    //    ssc.queueStream(rddQueue)
    inRDD
  }

  def generateRawDataStream(dataStream: DStream[String]): DStream[RawData] = {
    dataStream.transform { rawStream =>
      rawStream.map { line =>
        val row: Array[String] = line.split(",")
        RawData(row(0).toInt, row(1).toDouble, row(2).toDouble, row(3).toDouble, row(4).toDouble, row(5).toDouble,
          row(6).toDouble)
      }
    }
  }

  def generateAndProcessMetrics(stream: DStream[RawData]): Unit = {
    // Each RDD is in interval of 5/10 seconds. So group on the basis of driver, generate the metrics
    // and call the models
    val db = new DBClient

    val pubNubApi = new PubNubApi
    stream.foreachRDD { rdd =>
      val metricsDF: DataFrame = MetricsGenerator.genMetricsForInterval(rdd)
      metricsDF.show()
      //metricsDF.printSchema()
      metricsDF.collect().foreach { activity =>

        val redisClient: RedisClient = new RedisClient("localhost", 32769)
        val cacheLayer: CacheLayer = new application.CacheLayer(redisClient)
        val scoreGenerator = new ScoreGenerator

        // Get lat/lng for this time frame for the driver
        val lat = activity.getDouble(9)
        val lng = activity.getDouble(10)

        val driverId = activity.getInt(0)
        val isHA: Boolean = activity.getBoolean(11)
        val isHD: Boolean = activity.getBoolean(12)
        val isSLT: Boolean = activity.getBoolean(13)
        val isSRT: Boolean = activity.getBoolean(14)
        val isSLC: Boolean = activity.getBoolean(15)

        insertToIncidentTable(db, driverId, isHA, isHD, isSLT, isSRT, isSLC)

        //get Metrics Counter
        val prevMetricsOptions: Option[MetricsCounter] = cacheLayer.getMetricsCounters(driverId)
        if (prevMetricsOptions.isDefined) {
          val prevMetrics = prevMetricsOptions.get
          val newMetrics =
            MetricsCounter(
              prevMetrics.cntHA + boolToInt(isHA),
              prevMetrics.cntHD + boolToInt(isHD),
              prevMetrics.cntSLT + boolToInt(isSLT),
              prevMetrics.cntSRT + boolToInt(isSRT),
              prevMetrics.cntSLC + boolToInt(isSLC),
              prevMetrics.total + 1
            )

          //            // Update Metrics to cache (redis)
          cacheLayer.setMetricsCounters(driverId, newMetrics)
          val score = scoreGenerator.getScore(newMetrics)
          db.updateAvgRating(driverId, score.toFloat)
          //            println("driverId: "+driverId+" Score: "+score)

          if (score < Constants.RASH_SCORE_THRESHOLD) {
            db.insertScore(driverId, score.toFloat)
            val msgString = s"""{"driverId":$driverId,"lat":$lat,"lng":$lng,"score":$score}"""
            pubNubApi.sendMessage(Constants.MAP_PUBNUB_CHANNEL, msgString)
          }

        } else {
          val newMetrics =
            MetricsCounter(
              boolToInt(isHA),
              boolToInt(isHD),
              boolToInt(isSLT),
              boolToInt(isSRT),
              boolToInt(isSLC),
              1
            )

          // Update Metrics to cache (redis)
          cacheLayer.setMetricsCounters(driverId, newMetrics)
          val score = scoreGenerator.getScore(newMetrics)
          db.updateAvgRating(driverId, score.toFloat)
          if (score < Constants.RASH_SCORE_THRESHOLD) {
            db.insertScore(driverId, score.toFloat)
            val msgString = s"""{"driverId":$driverId,"lat":$lat,"lng":$lng,"score":$score}"""
            pubNubApi.sendMessage(Constants.MAP_PUBNUB_CHANNEL, msgString)
          }
        }
      }
      //
    }
  }

  def insertToIncidentTable(db: DBClient, driverId: Int, isHA: Boolean, isHD: Boolean, isSLT: Boolean, isSRT: Boolean,
                            isSLC: Boolean) = {
    println("Inserting to DB for driver " + driverId)
    if (isHA)
      db.insertIncident(driverId, IncidentTypes.HA)
    if (isHD)
      db.insertIncident(driverId, IncidentTypes.HD)
    if (isSLT)
      db.insertIncident(driverId, IncidentTypes.SLT)
    if (isSRT)
      db.insertIncident(driverId, IncidentTypes.SRT)
    if (isSLC)
      db.insertIncident(driverId, IncidentTypes.SLC)
    println("..DB insertion complete")
  }

  def boolToInt(x: Boolean): Int = {
    if (x) 1 else 0
  }

  def startStreaming: Unit = {
    ssc.start()
  }

  def stopStreaming: Unit = {
    ssc.stop(stopSparkContext = true)
  }

  def awaitTermination: Unit = {
    ssc.awaitTermination()
  }
}

object StreamProcess {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("safepath-streaming-engine").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val sp: StreamProcess = new StreamProcess(conf, ssc)
    val myStream: DStream[RawData] = sp.generateRawDataStream(sp.readStream("/Users/shivendrasoni/experiments/inout/safepath-engine/data-stream/data/"))

    sp.generateAndProcessMetrics(myStream)

    sp.startStreaming
    sp.awaitTermination
  }
}
