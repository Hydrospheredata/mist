package com.provectus.mist.test

import com.typesafe.config.ConfigFactory

object TestConfig{
  private val testconfig = ConfigFactory.load()

  val mqtt_test_sub_name = testconfig.getString("mist.test.mqtt.sub.name")
  val mqtt_test_pub_name = testconfig.getString("mist.test.mqtt.pub.name")
  val mqtt_request_jar = testconfig.getString("mist.test.mqtt.request.jar")
  val mqtt_try_response = testconfig.getString("mist.test.mqtt.response")

  val http_url = testconfig.getString("mist.test.http.url")
  val http_request_jar = testconfig.getString("mist.test.http.request.jar")
  val http_request_pyspark = testconfig.getString("mist.test.http.request.pyspark")
  val http_request_sparksql = testconfig.getString("mist.test.http.request.sparksql")
  val http_request_pysparksql = testconfig.getString("mist.test.http.request.pysparksql")

}