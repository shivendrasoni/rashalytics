package org.vikings.hackathons.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, SparkSession}
import org.vikings.hackathons.application.RawData
import org.vikings.hackathons.utils.Constants
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit

object MetricsGenerator {

  val KEY: String = "driverIdentifier"

  def genMetricsForInterval(intervalStream: RDD[RawData]): DataFrame = {
    val spark: SparkSession = SparkSession.builder.config(intervalStream.sparkContext.getConf).getOrCreate()
    import spark.implicits._
    val streamDS: Dataset[RawData] = intervalStream.toDS()

    val groupedByDriver: RelationalGroupedDataset = streamDS.groupBy(KEY)

    val avgDF = groupedByDriver
      .avg("pitch", "roll", "acc_y", "acc_x")
      .toDF(KEY, "avgPitch", "avgRoll", "avgAccY", "avgAccX")
    val maxRollDF = groupedByDriver.max("roll").toDF(KEY, "maxRoll")
    val minRollDF = groupedByDriver.min("roll").toDF(KEY, "minRoll")
    val maxAccX = groupedByDriver.max("acc_x").toDF(KEY, "maxAccX")
    val minAccX = groupedByDriver.min("acc_x").toDF(KEY, "minAccX")

    val maxLat = groupedByDriver.max("lat").toDF(KEY, "lat")
    val maxLng = groupedByDriver.max("lng").toDF(KEY, "lng")

    val statsDF = avgDF.join(maxRollDF, KEY).join(minRollDF, KEY).join(maxAccX, KEY).join(minAccX, KEY)
      .join(maxLat, KEY).join(maxLng,KEY)

    // Register UDFs
    val isHardAccelerationUDF = spark.udf.register("isHardAccelerationUDF", isHardAcceleration(_: Double, _: Double, _: Boolean))
    val isSharpTurnUDF = spark.udf.register("isSharpTurnUDF", isSharpTurn(_: Double, _: Double, _: String))
    val isSharpLaneChangeUDF = spark.udf.register("isSharpLaneChangeUDF", isSharpLaneChange(_: Double, _: Double, _: Double, _: Double))


    val metricsDF = statsDF
      .withColumn("isHardAcceleration", isHardAccelerationUDF($"avgPitch", $"avgAccY", lit(false)))
      .withColumn("isHardDeceleration", isHardAccelerationUDF($"avgPitch", $"avgAccY", lit(true)))
      .withColumn("isSharpLeftTurn", isSharpTurnUDF($"avgRoll", $"avgAccX", lit(Constants.LEFT)))
      .withColumn("isSharpRightTurn", isSharpTurnUDF($"avgRoll", $"avgAccX", lit(Constants.RIGHT)))
      .withColumn("isSharpLaneChange", isSharpLaneChangeUDF($"maxRoll", $"minRoll", $"maxAccX", $"minAccX"))

    // Printing metrics DataFrame
    metricsDF.show
    metricsDF
  }


  def isHardAcceleration(avgPitch: Double, avgAccY: Double, isDeceleration: Boolean): Boolean = {
    if (!isDeceleration)
      avgPitch < Constants.PITCH_THRESHOLD_MIN && avgAccY > Constants.ACC_Y_AVG_THRESHOLD_MAX
    else
      avgPitch > Constants.PITCH_THRESHOLD_MAX && avgAccY < Constants.ACC_Y_AVG_THRESHOLD_MIN
  }

  def isSharpTurn(avgRoll: Double, avgAccX: Double, direction: String): Boolean = {
    direction match {
      case Constants.LEFT =>
        avgRoll > Constants.AVG_ROLL_MAX_THRESHOLD && avgAccX < Constants.ACC_X_AVG_THRESHOLD_MIN
      case Constants.RIGHT =>
        avgRoll < Constants.AVG_ROLL_MIN_THRESHOLD && avgAccX > Constants.ACC_X_AVG_THRESHOLD_MAX
      case _ => throw new RuntimeException("Invalid Turn Direction")
    }
  }

  def isSharpLaneChange(maxRoll: Double, minRoll: Double, maxAccX: Double, minAccX: Double): Boolean = {
    Math.abs(maxRoll - minRoll) > Constants.AVG_ROLL_MAX_THRESHOLD &&
      Math.abs(maxAccX - minAccX) > Constants.ACC_X_AVG_THRESHOLD_MAX
  }
}
