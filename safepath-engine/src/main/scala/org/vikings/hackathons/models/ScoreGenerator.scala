package org.vikings.hackathons.models

import org.vikings.hackathons.streaming.MetricsCounter

class ScoreGenerator extends Serializable {

  def score(accPenalty: Double, deccPenalty: Double, turnPenalty: Double): Double = {
    1000 - (accPenalty + deccPenalty + turnPenalty)
  }

  def accelerationPenalty(badAcc: Double, totAcc: Double): Double = {
    100 * (badAcc / totAcc)
  }

  def turnPenalty(badLeftTurn: Double, badRightTurn: Double, badLaneChange: Double, totTurn: Double): Double = {
    100 * ((badLeftTurn + badRightTurn + badLaneChange) / totTurn)
  }

  def getScore(counts: MetricsCounter): Double = {
    score(
      accPenalty = accelerationPenalty(counts.cntHA, counts.total),
      deccPenalty = accelerationPenalty(counts.cntHD, counts.total),
      turnPenalty = turnPenalty(counts.cntSLT, counts.cntSRT, counts.cntSLC, counts.total)
    )
  }
}
