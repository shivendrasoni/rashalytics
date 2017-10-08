package org.vikings.hackathons.db

import java.sql.{Connection, DriverManager, Time}


class DBClient {
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://localhost/safepath"
  val username = "root"
  val password = "12345678"
  var connection: Connection = DriverManager.getConnection(url, username, password)

  //TODO Create Daemon for this (cache sync)

  def createConnection(): Unit = {
    connection = DriverManager.getConnection(url, username, password)
  }

  def closeConnection(): Unit = {
    connection.close()
  }

  def insertIncident(driverId: Int, incidentType: String): Unit = {
    Class.forName(driver)

    val query: String = s"""insert into incident_log (driver_id,incident_type) values(?,?)"""
    val statement = connection.prepareStatement(query)
    statement.setInt(1, driverId)
    statement.setString(2, incidentType)
    statement.execute()
  }

  def insertScore(driverId: Int, score: Float): Unit = {
    Class.forName(driver)

    val query: String = s"""insert into score_log (driver_id,score) values(?,?)"""
    val statement = connection.prepareStatement(query)
    statement.setInt(1, driverId)
    statement.setFloat(2, score)
    statement.execute()

//    connection.commit()
  }

  def getDriverProfile(driverId: Int): (String, String, Double) = {
    Class.forName(driver)
    val query = s"""select name, phone_number, average_rating from driver where id=${driverId.toString}"""
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(query)
    if (resultSet.next()) {
      val name = resultSet.getString("name")
      val phn = resultSet.getString("phone_number")
      val score = resultSet.getFloat("average_rating")
      (name, phn, score)
    } else {
      ("n/a", "n/a", 0.0)
    }
  }

  def updateAvgRating(driverId: Int, newScore: Float): Unit = {
    Class.forName(driver)
    val q1 =s"""select average_rating from driver where id = $driverId"""
    val statement1 = connection.createStatement()
    val resultSet1 = statement1.executeQuery(q1)
    if (resultSet1.next()) {
      val oldRating = resultSet1.getString("average_rating")
      val query: String = s"""update driver set average_rating =? where id = ?"""
      val statement = connection.prepareStatement(query)
      statement.setInt(1, Math.round(oldRating.toFloat + newScore) / 2)
      statement.setInt(2, driverId)
      statement.execute()
    }
//    connection.commit()
  }

  def getIncidents(driverId: Int): String = {
    Class.forName(driver)
    //    val countsPerHourQuery =
    //      "select count(incident_log.incident_type) as cnt, TIME_FORMAT(incident_log.timestamp,\"%H\") as hourly, max(score_log.score) as score from incident_log, score_log where incident_log.driver_id = score_log.driver_id group by hourly"
    val countsPerHourQuery =
    s"select count(incident_log.incident_type) as cnt, MINUTE(incident_log.timestamp) as ihourly,MINUTE(score_log.timestamp) as shourly,max(score_log.score) as score from incident_log,score_log where MINUTE(incident_log.timestamp) = MINUTE(score_log.timestamp) and incident_log.driver_id = score_log.driver_id and incident_log.driver_id =$driverId group by ihourly,shourly limit 25"
    val countsList: scala.collection.mutable.ListBuffer[(Int, Int, Int)] =
      scala.collection.mutable.ListBuffer[(Int, Int, Int)]()
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(countsPerHourQuery)
    while (resultSet.next()) {
      countsList += Tuple3((resultSet.getInt("ihourly") % 24) + 1, resultSet.getInt("cnt"), resultSet.getInt("score"))
    }
    var myOut: String = "["
    for (i <- countsList.indices) {
      val (hour, cnt, score) = countsList(i)
      myOut +=
        s"""{"hour":$hour, "incidentCount":$cnt, "score":$score},""".stripMargin
    }
    myOut = myOut.dropRight(1)
    if(myOut.isEmpty)
      ""
    else {
      myOut += "]"
      myOut
    }
  }

  def getIncidentType(driverId: Int): String = {
    Class.forName(driver)
    val query =
      s"""select count(*) cnt, incident_type from incident_log where driver_id = ${driverId.toString} group by incident_type;"""
    val incidentCountList: scala.collection.mutable.ListBuffer[(String, Int)] =
      scala.collection.mutable.ListBuffer[(String, Int)]()
    val statement = connection.createStatement()
    var str = "["
    val resultSet = statement.executeQuery(query)
    var tot = 0
    while (resultSet.next()) {
      val cnt = resultSet.getInt("cnt")
      incidentCountList += Tuple2(resultSet.getString("incident_type"), cnt)
      tot += cnt
    }
    var sum = 0.0
    for (i <- incidentCountList.indices) {
      val (incidentType, cnt) = incidentCountList(i)
      val countPercent = Math.floor((100.0 * cnt) / tot.toFloat)
      sum += countPercent
      str += s"""{"name":"$incidentType","value":$countPercent},"""

    }
    if (sum < 100.0)
      str += s"""{"name":"other","value":${100 - sum}}"""
    else
      str = str.dropRight(1)
    str += "]"
    str
  }

  def getAllScore: List[(Double, Double, Double)] = {
    val query = "select driver_id, timestamp, score from score_log"
    val list: scala.collection.mutable.ListBuffer[(Double, Double, Double)] =
      scala.collection.mutable.ListBuffer[(Double, Double, Double)]()
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(query)
    while (resultSet.next()) {
      list += Tuple3(resultSet.getInt("driver_id"), resultSet.getTime("timestamp").toLocalTime.getMinute,
        Math.round(resultSet.getFloat("score")))
    }
    list.toList
  }

  def getPrevHours(driverId: Int): Map[Int, Float] = {
    val query = s"select HOUR(timestamp) as hour,max(score) as score from score_log where driver_id=$driverId group by hour"
    var myMap = Map.empty[Int,Float]
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(query)
    while (resultSet.next()) {
     myMap = myMap ++ Map(resultSet.getInt("hour") -> resultSet.getFloat("score"))
    }
    myMap
  }
}

object DBClient {
  def main(args: Array[String]): Unit = {

    val dbc = new DBClient
//    val str = dbc.getIncidents(1)
//    println(str)
//    dbc.updateAvgRating(1, 912)

//    dbc.insertScore(1, 910)
    println(dbc.getPrevHours(1))
  }


}

