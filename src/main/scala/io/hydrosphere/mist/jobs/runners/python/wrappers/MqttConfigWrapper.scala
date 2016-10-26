package io.hydrosphere.mist.jobs.runners.python.wrappers

import io.hydrosphere.mist.MistConfig

private[mist] class MqttConfigWrapper {
  def IsOn: Boolean = MistConfig.MQTT.isOn
  def getHost: String = MistConfig.MQTT.host
  def getPort: Int = MistConfig.MQTT.port
  def getTopic: String = MistConfig.MQTT.publishTopic
}