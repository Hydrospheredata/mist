package io.hydrosphere.mist.lib

trait StreamingMistJob extends SparkStreamingSupport with JobMqtt {

  protected var _id: String = null

  def setJobId(id: String) = _id = id

  override def publishToMqtt(message: String) = {
    super.publishToMqtt(s"${_id}: " + message)
  }

  def doStuff(): Unit
}
