package org.vikings.hackathons.webservice


import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{Row, SparkSession}
import org.vikings.hackathons.db.DBClient


@Path("/")
class Resource {

  private val dbClient = new DBClient



  @GET
  @Path("/{driverId}/profile/")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getProfile(@PathParam("driverId") driverId: Int): Response = {
    val (name, phn, score) = dbClient.getDriverProfile(driverId)
    val json = s"""{"driverId":$driverId,"driverName":"$name","driverScore":$score,"driverPhone":$phn}"""
    Response.status(200).entity(json)
      .header("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
      .header("Access-Control-Allow-Origin","*")
      .build()
  }

  @GET
  @Path("/{driverId}/incidents/")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getIncidents(@PathParam("driverId") driverId: Int): Response = {
    val json = dbClient.getIncidents(driverId)
    Response.status(200).entity(json)
      .header("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
      .header("Access-Control-Allow-Origin","*")
      .build()
  }

  @GET
  @Path("/{driverId}/incidentTypes/")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getIncidentTypes(@PathParam("driverId") driverId: Int): Response = {
    val json = dbClient.getIncidentType(driverId)
    Response.status(200).entity(json)
      .header("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
      .header("Access-Control-Allow-Origin","*")
      .build()
  }

  @GET
  @Path("/heartbeat")
  def heartbeat(): Response = {
    println("Call Received....")
    val json = s"""{"status": "chal raha hai :) "}"""
    Response.status(200).entity(json)
      .header("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
      .header("Access-Control-Allow-Origin","*")
      .build()
  }

  @GET
  @Path("/{driverId}/forecast/")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getForecast(@PathParam("driverId") driverId: Int): Response = {

    val spark = SparkSession.builder.getOrCreate()
    val myMap = dbClient.getPrevHours(driverId)

    val model = PipelineModel.load("/tmp/scoreModel")

    val prevList = myMap.toList.sortBy(_._1)
    val lastHour = prevList.last._1
    if(lastHour < 24) {
      val newList = (lastHour+1 to 24).map(x => (driverId, x))
      val testDF = spark.createDataFrame(newList).toDF("driverId","hour")
      val outList = model.transform(testDF).collect().map(x => (x.getInt(1),x.getDouble(4)))

      var json ="["
      for(i <- prevList.indices) {
        json+= s"""{"hour":${prevList(i)._1},"fact":${prevList(i)._2}},"""
      }
      for(i <- outList.indices) {
        json+= s"""{"hour":${outList(i)._1},"prediction":${outList(i)._2}},"""
      }
      json = json.dropRight(1)
      json +="]"
      Response.status(200).entity(json)
        .header("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
        .header("Access-Control-Allow-Origin","*")
        .build()
    } else {
      Response.status(200).entity("n/a")
        .header("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
        .header("Access-Control-Allow-Origin","*")
        .build()
    }

  }

}
