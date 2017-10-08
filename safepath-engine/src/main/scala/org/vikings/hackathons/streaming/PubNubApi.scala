package org.vikings.hackathons.streaming

import com.pubnub.api.{Pubnub, Callback, PubnubError}

class PubNubApi {

  val PUB_KEY = "pub-c-6546afcf-2977-4f18-9ab4-68543a990569"
  val SUB_KEY = "sub-c-9759ad36-68e5-11e5-9e44-02ee2ddab7fe"

  def sendMessage(channelName: String, msg: String): Unit = {

    val pubnub = new Pubnub(PUB_KEY,SUB_KEY)
    val callback = new Callback() {
      override def successCallback(channel: String, response: Object): Unit =
        println(response.toString)

      override def errorCallback(channel: String, pubnubError: PubnubError): Unit =
        println(pubnubError.getErrorString)
    }
    pubnub.publish(channelName, msg, callback)
    Thread.sleep(1000)
    pubnub.shutdown()

  }
}
