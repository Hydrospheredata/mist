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
  val contextWrapper = ContextBuilder.namedSparkContext("foo")

  override def beforeAll(): Unit = {
    Thread.sleep(5000)
  }

  def testServing(modelConfig: String)( predicate: ( Either[Map[String, Any], String] => Unit)): Unit = {
    val json = modelConfig.parseJson
    val jobConfiguration = json.convertTo[ServingJobConfiguration]
    val serveJob = Runner(jobConfiguration, contextWrapper)
    predicate(serveJob.run())
  }

  def extractResult(data: Map[String, Any]): List[Map[String, Double]] = {
    data("result").asInstanceOf[List[Map[String, Double]]]
  }

  test("Local Binarizer test") {
    testServing(TestConfig.LocalModels.binarizer) {
      case Left(data) =>
        val resList = extractResult(data)
        val threshold = 5.0
        resList.foreach { x =>
          val reference = if (x("feature") > threshold) 1.0 else 0.0
          assert(reference === x("binarized_feature"))
        }
      case Right(error) =>
        assert(false, error)
    }
  }

  test("Local Decision Tree Classification pipeline test") {
    testServing(TestConfig.LocalModels.treeClassifier_1) {
      case Left(data) =>
        val resList = extractResult(data)
        resList foreach { map =>
          assert(map("predictedLabel").toString.toDouble === 1.0)
        }
      case Right(error) =>
        assert(false, error)
    }
    testServing(TestConfig.LocalModels.treeClassifier_0) {
      case Left(data) =>
        val resList = extractResult(data)
        resList foreach { map =>
          assert(map("predictedLabel").toString.toDouble === 0.0)
        }
      case Right(error) =>
        assert(false, error)
    }
  }

  override def afterAll(): Unit ={
    contextWrapper.stop()

    Thread.sleep(5000)
  }
}
