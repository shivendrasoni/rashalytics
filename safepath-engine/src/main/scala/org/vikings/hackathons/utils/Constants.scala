package org.vikings.hackathons.utils

object Constants {

  val PITCH_THRESHOLD_MIN: Double = -0.14
  val ACC_Y_AVG_THRESHOLD_MAX: Double = 0.38
  val PITCH_THRESHOLD_MAX: Double = 0.25
  val ACC_Y_AVG_THRESHOLD_MIN: Double = -1.0
  val AVG_ROLL_MIN_THRESHOLD: Double = -0.3
  val AVG_ROLL_MAX_THRESHOLD: Double = 0.3
  val ACC_X_AVG_THRESHOLD_MAX: Double = 1.5
  val ACC_X_AVG_THRESHOLD_MIN: Double = -1.5

  val RASH_SCORE_THRESHOLD: Double = 950.0

  val MAP_PUBNUB_CHANNEL: String = "map_channel"

  val LEFT: String = "left"
  val RIGHT: String = "right"

}
