package io.hydrosphere.mist

import com.typesafe.config.ConfigFactory

object TestConfig{
  private val testconfig = ConfigFactory.load()

  val assemblyjar = testconfig.getString("mist.test.assemblyjar")

  val other_context_name = testconfig.getString("mist.test.othercontextname")

  val mqtt_test_sub_name = testconfig.getString("mist.test.mqtt.sub.name")
  val mqtt_test_pub_name = testconfig.getString("mist.test.mqtt.pub.name")

  val http_url = testconfig.getString("mist.test.http.url")
  val http_url_it = testconfig.getString("mist.test.http.url_it")

  val request_test_timeout = testconfig.getString("mist.test.request.testtimeout")
  val request_jar = testconfig.getString("mist.test.request.jar")
  val request_testerror = testconfig.getString("mist.test.request.testerror")
  val request_pyspark = testconfig.getString("mist.test.request.pyspark")
  val request_sparksql = testconfig.getString("mist.test.request.sparksql")
  val request_sparkhive = testconfig.getString("mist.test.request.sparkhive")
  val request_pysparksql = testconfig.getString("mist.test.request.pysparksql")
  val request_pysparkhive = testconfig.getString("mist.test.request.pysparkhive")
  val request_sparksession = testconfig.getString("mist.test.request.sparksession")
  val request_pysparksession = testconfig.getString("mist.test.request.pysparksession")

  val request_jar_other_context = testconfig.getString("mist.test.request.jarother")

  val request_pyerror = testconfig.getString("mist.test.request.pyerror")
  val request_nodostuff = testconfig.getString("mist.test.request.nodostuff")
  val request_badpatch = testconfig.getString("mist.test.request.badpatch")
  val request_badextension = testconfig.getString("mist.test.request.badextension")
  val request_bad = testconfig.getString("mist.test.request.badrequest")
  val request_badjson = testconfig.getString("mist.test.request.badjson")

  val request_jar_disposable_context = testconfig.getString("mist.test.request.disposable")
  val disposable_context_name = testconfig.getString("mist.test.disposable_context_name")
}
