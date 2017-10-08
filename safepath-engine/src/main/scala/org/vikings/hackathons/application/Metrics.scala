package org.vikings.hackathons.application

case class Metrics(lat: Double,
                   long: Double,
                   hardAcceleration: Boolean,
                   hardDeceleration: Boolean,
                   sharpLeftTurn: Boolean,
                   sharpRightTurn: Boolean,
                   sharpLaneChange: Boolean,
                   densityScore: Int, // (0-10) 10 being the most crowded(wrt traffic)
                   speedLimit: Double, // Google Roads API
                   speed: Double
                  )
