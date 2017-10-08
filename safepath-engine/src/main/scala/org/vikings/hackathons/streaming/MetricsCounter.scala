package org.vikings.hackathons.streaming

case class MetricsCounter(cntHA: Int, cntHD: Int, cntSLT: Int, cntSRT: Int, cntSLC: Int, total: Int) extends Serializable
