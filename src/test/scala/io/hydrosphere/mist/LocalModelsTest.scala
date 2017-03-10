package io.hydrosphere.mist

import io.hydrosphere.mist.contexts.ContextBuilder
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.jobs.{FullJobConfiguration, MistJobConfiguration, ServingJobConfiguration}
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
import org.apache.spark.mllib.linalg.DenseVector
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

  test("Local Random Forest Classification pipeline test") {
    testServing(TestConfig.LocalModels.forestClassifier_1) {
      case Left(data) =>
        val resList = extractResult(data)
        resList foreach { map =>
          assert(map("predictedLabel").toString.toDouble === 1.0)
        }
      case Right(error) =>
        assert(false, error)
    }
    testServing(TestConfig.LocalModels.forestClassifier_0) {
      case Left(data) =>
        val resList = extractResult(data)
        resList foreach { map =>
          assert(map("predictedLabel").toString.toDouble === 0.0)
        }
      case Right(error) =>
        assert(false, error)
    }
  }

  test("Local PCA test") {
    testServing(TestConfig.LocalModels.pca) {
      case Left(data) =>
        val validation = Array(
          List(-4.645104331781534,-1.1167972663619026,-5.524543751369387),
          List(-6.428880535676489,-5.337951427775355,-5.524543751369389)
        )
        val resList = data("result").asInstanceOf[List[Map[String, Any]]]

        resList zip validation foreach { x =>
          val res = x._1("pcaFeatures").asInstanceOf[DenseVector].toArray
          res zip x._2 foreach { y =>
            assert(Math.abs(y._1 - y._2) < 0.000001)
          }
        }
      case Right(error) =>
        assert(false, error)
    }
  }

  test("Local MinMaxScaler test") {
    testServing(TestConfig.LocalModels.minmaxscaler) {
      case Left(data) =>
        val validation = Array(
          List(0.0,-0.01,0.0),
          List(0.5,0.13999999999999999,0.5),
          List(1.0,0.1,1.0)
        )
        val resList = data("result").asInstanceOf[List[Map[String, Any]]]

        resList zip validation foreach { x =>
          val res = x._1("scaledFeatures").asInstanceOf[Array[Double]]
          res zip x._2 foreach { y =>
            assert(Math.abs(y._1 - y._2) < 0.000001)
          }
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
