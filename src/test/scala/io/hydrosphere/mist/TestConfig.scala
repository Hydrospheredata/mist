package io.hydrosphere.mist

import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.utils.SparkUtils

object TestConfig {
  private val testConfig = ConfigFactory.load()

  val examplesPath = if(SparkUtils.Version.isOldVersion) testConfig.getString("mist.test.examples.path_2_10") else  testConfig.getString("mist.test.examples.path_2_11")

  val assemblyJar = if(SparkUtils.Version.isOldVersion) testConfig.getString("mist.test.assemblyjar_2_10") else testConfig.getString("mist.test.assemblyjar_2_11")

  val otherContextName = testConfig.getString("mist.test.othercontextname")

  val mqttTestSubName = testConfig.getString("mist.test.mqtt.sub.name")
  val mqttTestPubName = testConfig.getString("mist.test.mqtt.pub.name")

  val httpUrl = testConfig.getString("mist.test.http.url")
  val httpUrlIt = testConfig.getString("mist.test.http.url_it")
  val restificatedUrl = testConfig.getString("mist.test.http.restificated_url")
  val restUIUrlListWorkers = testConfig.getString("mist.test.http.rest_ui_url_workers")
  val restUIUrlListJobs = testConfig.getString("mist.test.http.rest_ui_url_jobs")
  val restUIUrlListRouters = testConfig.getString("mist.test.http.rest_ui_url_routers")

  val requestTestTimeout = if(SparkUtils.Version.isOldVersion) testConfig.getString("mist.test.request.testtimeout_2_10") else testConfig.getString("mist.test.request.testtimeout_2_11")

  val requestJar = if(SparkUtils.Version.isOldVersion) testConfig.getString("mist.test.request.jar_2_10") else testConfig.getString("mist.test.request.jar_2_11")
  val requestHdfsJar = if(SparkUtils.Version.isOldVersion) testConfig.getString("mist.test.request.hdfs_jar_2_10") else testConfig.getString("mist.test.request.hdfs_jar_2_11")
  val requestTesterror = if(SparkUtils.Version.isOldVersion) testConfig.getString("mist.test.request.testerror_2_10") else testConfig.getString("mist.test.request.testerror_2_11")
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

  val requestJarOtherContext = if(SparkUtils.Version.isOldVersion) testConfig.getString("mist.test.request.jarother_2_10") else testConfig.getString("mist.test.request.jarother_2_11")
  val requestPyError = testConfig.getString("mist.test.request.pyerror")
  val requestBadPath = testConfig.getString("mist.test.request.badpath")
  val requestBadExtension = testConfig.getString("mist.test.request.badextension")
  val requestBad = testConfig.getString("mist.test.request.badrequest")
  val requestBadJson = testConfig.getString("mist.test.request.badjson")

  val requestJarDisposableContext = if(SparkUtils.Version.isOldVersion) testConfig.getString("mist.test.request.disposable_2_10") else testConfig.getString("mist.test.request.disposable_2_11")
  val disposableContextName = testConfig.getString("mist.test.disposable_context_name")

  val integrationConf = testConfig.getString("mist.test.integration_conf")
  val cliConfig = if(SparkUtils.Version.isOldVersion) testConfig.getString("mist.test.cli_conf_2_10") else testConfig.getString("mist.test.cli_conf_2_11")
  val restUIConfig = if(SparkUtils.Version.isOldVersion) testConfig.getString("mist.test.rest_ui_2_10") else testConfig.getString("mist.test.rest_ui_2_11")

  object LocalModels {
    val binarizer = testConfig.getString("mist.test.ml.binarizer")
    val pca = testConfig.getString("mist.test.ml.pca")
    val minMaxScaler = testConfig.getString("mist.test.ml.minmaxscaler")
    val standardScaler = testConfig.getString("mist.test.ml.standardscaler")
    val maxAbsScaler = testConfig.getString("mist.test.ml.maxabsscaler")
    val stringIndexer = testConfig.getString("mist.test.ml.stringindexer")
    val oneHotEncoder = testConfig.getString("mist.test.ml.onehotencoder")
    val ngram = testConfig.getString("mist.test.ml.ngram")
    val stopwordsremover = testConfig.getString("mist.test.ml.stopwordsremover")
    val normalizer = testConfig.getString("mist.test.ml.normalizer")
    val polynomialExpansion = testConfig.getString("mist.test.ml.polynomialexpansion")
    val dct = testConfig.getString("mist.test.ml.dct")
    val tfidf = testConfig.getString("mist.test.ml.tfidf")

    val naiveBayes = testConfig.getString("mist.test.ml.naivebayes")
    val gbtregressor = testConfig.getString("mist.test.ml.gbtregressor")

    val treeClassifier_0 = testConfig.getString("mist.test.ml.tree-classifier_0")
    val treeClassifier_1 = testConfig.getString("mist.test.ml.tree-classifier_1")

    val treeRegressor_0 = testConfig.getString("mist.test.ml.tree-regressor_0")
    val treeRegressor_1 = testConfig.getString("mist.test.ml.tree-regressor_1")

    val forestClassifier_0 = testConfig.getString("mist.test.ml.forest-classifier_0")
    val forestClassifier_1 = testConfig.getString("mist.test.ml.forest-classifier_1")

    val forestRegressor_0 = testConfig.getString("mist.test.ml.forest-regressor_0")
    val forestRegressor_1 = testConfig.getString("mist.test.ml.forest-regressor_1")
  }

}
