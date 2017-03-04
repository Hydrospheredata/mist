package io.hydrosphere.mist

import com.typesafe.config.ConfigFactory

object TestConfig {
  private val testConfig = ConfigFactory.load()

  val versionRegex = "(\\d+)\\.(\\d+).*".r
  val sparkVersion = util.Properties.propOrNone("sparkVersion").getOrElse("[1.5.2, )")

  val OldVersion = {
    sparkVersion match {
      case versionRegex(major, minor) if major.toInt == 1 && List(4, 5, 6).contains(minor.toInt) => true
      case versionRegex(major, minor) if major.toInt > 1 => false
      case _ => true
    }
  }

  val examplesPath = if(OldVersion) testConfig.getString("mist.test.examples.path_2_10") else  testConfig.getString("mist.test.examples.path_2_11")

  val assemblyJar = if(OldVersion) testConfig.getString("mist.test.assemblyjar_2_10") else testConfig.getString("mist.test.assemblyjar_2_11")

  val otherContextName = testConfig.getString("mist.test.othercontextname")

  val mqttTestSubName = testConfig.getString("mist.test.mqtt.sub.name")
  val mqttTestPubName = testConfig.getString("mist.test.mqtt.pub.name")

  val httpUrl = testConfig.getString("mist.test.http.url")
  val httpUrlIt = testConfig.getString("mist.test.http.url_it")
  val restificatedUrl = testConfig.getString("mist.test.http.restificated_url")
  val restUIUrlListWorkers = testConfig.getString("mist.test.http.rest_ui_url_workers")
  val restUIUrlListJobs = testConfig.getString("mist.test.http.rest_ui_url_jobs")
  val restUIUrlListRouters = testConfig.getString("mist.test.http.rest_ui_url_routers")

  val requestTestTimeout = if(OldVersion) testConfig.getString("mist.test.request.testtimeout_2_10") else testConfig.getString("mist.test.request.testtimeout_2_11")

  val requestJar = if(OldVersion) testConfig.getString("mist.test.request.jar_2_10") else testConfig.getString("mist.test.request.jar_2_11")
  val requestHdfsJar = if(OldVersion) testConfig.getString("mist.test.request.hdfs_jar_2_10") else testConfig.getString("mist.test.request.hdfs_jar_2_11")
  val requestTesterror = if(OldVersion) testConfig.getString("mist.test.request.testerror_2_10") else testConfig.getString("mist.test.request.testerror_2_11")
  val requestPyspark = testConfig.getString("mist.test.request.pyspark")
  val requestPyMqttPublisher = testConfig.getString("mist.test.request.py_mqtt_pub")
  val requestSparkSql = testConfig.getString("mist.test.request.sparksql")
  val requestSparkhive = testConfig.getString("mist.test.request.sparkhive")
  val requestPysparkSql = testConfig.getString("mist.test.request.pysparksql")
  val requestPysparkHive = testConfig.getString("mist.test.request.pysparkhive")
  val requestSparkSession = testConfig.getString("mist.test.request.sparksession")
  val requestPysparkSession = testConfig.getString("mist.test.request.pysparksession")
  val requestPyHdfs = testConfig.getString("mist.test.request.hdfs_python")

  val asyncRestificatedRequest = testConfig.getString("mist.test.request.async-restificated")
  val restificatedRequest = testConfig.getString("mist.test.request.restificated")

  val requestJarOtherContext = if(OldVersion) testConfig.getString("mist.test.request.jarother_2_10") else testConfig.getString("mist.test.request.jarother_2_11")
  val requestPyError = testConfig.getString("mist.test.request.pyerror")
  val requestBadPath = testConfig.getString("mist.test.request.badpath")
  val requestBadExtension = testConfig.getString("mist.test.request.badextension")
  val requestBad = testConfig.getString("mist.test.request.badrequest")
  val requestBadJson = testConfig.getString("mist.test.request.badjson")

  val requestJarDisposableContext = if(OldVersion) testConfig.getString("mist.test.request.disposable_2_10") else testConfig.getString("mist.test.request.disposable_2_11")
  val disposableContextName = testConfig.getString("mist.test.disposable_context_name")

  val integrationConf = testConfig.getString("mist.test.integration_conf")
  val cliConfig = if(OldVersion) testConfig.getString("mist.test.cli_conf_2_10") else testConfig.getString("mist.test.cli_conf_2_11")
  val restUIConfig = if(OldVersion) testConfig.getString("mist.test.rest_ui_2_10") else testConfig.getString("mist.test.rest_ui_2_11")

  val MLBinarizer =testConfig.getString("mist.test.ml.binarizer")
}
