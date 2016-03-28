package com.provectus.mist.test

import com.typesafe.config.ConfigFactory

object TestConfig{
  private val testconfig = ConfigFactory.load()

  val mqtt_test_sub_name = testconfig.getString("mist.test.mqtt.sub.name")
  val mqtt_test_pub_name = testconfig.getString("mist.test.mqtt.pub.name")

  val http_url = testconfig.getString("mist.test.http.url")

  val request_test_timeout = testconfig.getString("mist.test.request.testtimeout")
  val request_jar = testconfig.getString("mist.test.request.jar")
  val request_testerror = testconfig.getString("mist.test.request.testerror")
  val request_pyspark = testconfig.getString("mist.test.request.pyspark")
  val request_sparksql = testconfig.getString("mist.test.request.sparksql")
  val request_sparkhive = testconfig.getString("mist.test.request.sparkhive")
  val request_pysparksql = testconfig.getString("mist.test.request.pysparksql")
  val request_pysparkhive = testconfig.getString("mist.test.request.pysparkhive")
  val request_jar_other_context = testconfig.getString("mist.test.request.jarother")
  val request_bad = testconfig.getString("mist.test.request.badrequest")
}