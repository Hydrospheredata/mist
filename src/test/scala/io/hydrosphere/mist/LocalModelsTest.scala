package io.hydrosphere.mist

import io.hydrosphere.mist.contexts.ContextBuilder
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.jobs.{FullJobConfiguration, MistJobConfiguration, ServingJobConfiguration}
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import org.scalatest.concurrent.Eventually
import spray.json.{DefaultJsonProtocol, pimpString}

import scala.concurrent.duration._
import collection.JavaConversions._


class LocalModelsTest extends FunSuite with Eventually with BeforeAndAfterAll with JobConfigurationJsonSerialization with DefaultJsonProtocol {
  val jobConfiguration_Empty = MistJobConfiguration("", "", "Empty Test Jobconfiguration", Map().empty, Option("1"))
  val jobConfiguration_Python = MistJobConfiguration("some.py", "", "Python Test Jobconfiguration", Map().empty, Option("2"))
  val jobConfiguration_Jar = MistJobConfiguration("some.jar", "", "Jar Test Jobconfiguration", Map().empty, Option("3"))
  val contextWrapper = ContextBuilder.namedSparkContext("foo")

  val versionRegex = "(\\d+)\\.(\\d+).*".r
  val sparkVersion = util.Properties.propOrNone("sparkVersion").getOrElse("[1.5.2, )")

  val checkSparkSessionLogic = {
    sparkVersion match {
      case versionRegex(major, minor) if major.toInt > 1 => true
      case _ => false
    }
  }

  override def beforeAll(): Unit = {
    Thread.sleep(5000)
  }

  test("Local Binarizer test") {
    val json = TestConfig.MLBinarizer.parseJson
    val jobConfiguration = json.convertTo[ServingJobConfiguration]
    val someJarJob = Runner(jobConfiguration, contextWrapper)
    val result = someJarJob.run()
    val resList = result.left.get("result").asInstanceOf[List[Map[String, Double]]]

    val threshold = 5.0
    resList.foreach { x =>
      val reference = if (x.get("feature").head > threshold) 1.0 else 0.0
      assert(reference == x.get("binarized_feature").head)
    }
  }

  override def afterAll(): Unit ={
    contextWrapper.stop()

    Thread.sleep(5000)
  }
}
