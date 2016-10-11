package io.hydrosphere.mist

import com.typesafe.config.ConfigFactory

object TestConfig{
  private val testconfig = ConfigFactory.load()

  val versionRegex = "(\\d+)\\.(\\d+).*".r
  val sparkVersion = util.Properties.propOrNone("sparkVersion").getOrElse("[1.5.2, )")

  val OldVersion = {
    sparkVersion match {
      case versionRegex(major, minor) if major.toInt == 1 && List(4, 5, 6).contains(minor.toInt) => true
      case versionRegex(major, minor) if major.toInt > 1 => false
      case _ => true
    }
  }
  val assemblyjar = if(OldVersion) testconfig.getString("mist.test.assemblyjar_2_10") else testconfig.getString("mist.test.assemblyjar_2_11")

  val other_context_name = testconfig.getString("mist.test.othercontextname")

  val mqtt_test_sub_name = testconfig.getString("mist.test.mqtt.sub.name")
  val mqtt_test_pub_name = testconfig.getString("mist.test.mqtt.pub.name")

  val http_url = testconfig.getString("mist.test.http.url")
  val http_url_it = testconfig.getString("mist.test.http.url_it")
  val restificatedUrl = testconfig.getString("mist.test.http.restificated_url")

  val request_test_timeout = if(OldVersion) testconfig.getString("mist.test.request.testtimeout_2_10") else testconfig.getString("mist.test.request.testtimeout_2_11")

  val request_jar = if(OldVersion) testconfig.getString("mist.test.request.jar_2_10") else testconfig.getString("mist.test.request.jar_2_11")
  val request_hdfs_jar = if(OldVersion) testconfig.getString("mist.test.request.hdfs_jar_2_10") else testconfig.getString("mist.test.request.hdfs_jar_2_11")
  val request_testerror = if(OldVersion) testconfig.getString("mist.test.request.testerror_2_10") else testconfig.getString("mist.test.request.testerror_2_11")
  val request_pyspark = testconfig.getString("mist.test.request.pyspark")
  val request_sparksql = testconfig.getString("mist.test.request.sparksql")
  val request_sparkhive = testconfig.getString("mist.test.request.sparkhive")
  val request_pysparksql = testconfig.getString("mist.test.request.pysparksql")
  val request_pysparkhive = testconfig.getString("mist.test.request.pysparkhive")
  val request_sparksession = testconfig.getString("mist.test.request.sparksession")
  val request_pysparksession = testconfig.getString("mist.test.request.pysparksession")
  val request_pyhdfs = testconfig.getString("mist.test.request.hdfs_python")

  val async_restificated_request = testconfig.getString("mist.test.request.async-restifcated")
  val restificatedRequest = testconfig.getString("mist.test.request.restificated")

  val request_jar_other_context = if(OldVersion) testconfig.getString("mist.test.request.jarother_2_10") else testconfig.getString("mist.test.request.jarother_2_11")
  val request_pyerror = testconfig.getString("mist.test.request.pyerror")
  val request_badpatch = testconfig.getString("mist.test.request.badpatch")
  val request_badextension = testconfig.getString("mist.test.request.badextension")
  val request_bad = testconfig.getString("mist.test.request.badrequest")
  val request_badjson = testconfig.getString("mist.test.request.badjson")

  val request_jar_disposable_context = if(OldVersion) testconfig.getString("mist.test.request.disposable_2_10") else testconfig.getString("mist.test.request.disposable_2_11")
  val disposable_context_name = testconfig.getString("mist.test.disposable_context_name")
}
