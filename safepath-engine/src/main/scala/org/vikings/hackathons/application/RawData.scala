package org.vikings.hackathons.application

import org.vikings.hackathons.utils.Driver

case class RawData(driverIdentifier: Int,
                   lat: Double,
                   lng: Double,
                   acc_x: Double,
                   acc_y: Double,
                   pitch: Double, // rotaional movement sideways
                   roll: Double // rotaional movement along y-axis
                  )