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

  test("Local Binarizer test") {

    val json = TestConfig.MLBinarizer.parseJson
    val jobConfiguration = json.convertTo[ServingJobConfiguration]
    val serveJob = Runner(jobConfiguration, contextWrapper)
    serveJob.run() match {
      case Left(data) =>
        val resList = data("result").asInstanceOf[List[Map[String, Double]]]
        val threshold = 5.0
        resList.foreach { x =>
          val reference = if (x("feature") > threshold) 1.0 else 0.0
          assert(reference == x("binarized_feature"))
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
