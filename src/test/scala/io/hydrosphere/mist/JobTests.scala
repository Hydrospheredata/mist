package  io.hydrosphere.mist
import java.io.{File, FileInputStream, FileOutputStream}

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import io.hydrosphere.mist.contexts.ContextBuilder
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.master._
import org.apache.commons.lang.SerializationUtils
import org.mapdb.{DBMaker, Serializer}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import spray.json.{DefaultJsonProtocol, DeserializationException, pimpString}
import org.scalatest._
import org.scalatest.time.{Second, Seconds, Span}
import sbt.Keys._

import scala.util.matching.Regex

class JobRepositoryTest extends FunSuite with Eventually with BeforeAndAfterAll with JsonFormatSupport with DefaultJsonProtocol {

  val jobConfiguration = new JobConfiguration(None, None, None, "Test Jobconfiguration", Map().empty, None)

  override def beforeAll(): Unit = {
    Thread.sleep(5000)
  }

  //InMapDbJobConfigurationRepository
  test("size InMapDbJobConfigurationRepository") {
    InMapDbJobConfigurationRepository.clear()
    InMapDbJobConfigurationRepository.add("1", jobConfiguration)
    InMapDbJobConfigurationRepository.add("2", jobConfiguration)
    InMapDbJobConfigurationRepository.add("3", jobConfiguration)
    assert(InMapDbJobConfigurationRepository.size == 3)
  }

  test("Clear InMapDbJobConfigurationRepository") {
    InMapDbJobConfigurationRepository.clear()
    assert(InMapDbJobConfigurationRepository.size == 0)
  }

  test("Add in InMapDbJobConfigurationRepository") {
    InMapDbJobConfigurationRepository.clear()
    InMapDbJobConfigurationRepository.add("1", jobConfiguration)
    assert(InMapDbJobConfigurationRepository.size == 1)
  }

  test("Remove from InMapDbJobConfigurationRepository") {
    InMapDbJobConfigurationRepository.clear()
    InMapDbJobConfigurationRepository.add("1", jobConfiguration)
    InMapDbJobConfigurationRepository.remove("1")
    assert(InMapDbJobConfigurationRepository.size == 0)
  }

  test("get from InMapDbJobConfigurationRepository") {
    InMapDbJobConfigurationRepository.clear()
    InMapDbJobConfigurationRepository.add("1", jobConfiguration)
    assert( InMapDbJobConfigurationRepository.get("1").name == jobConfiguration.name)
  }

  test("getAll from InMapDbJobConfigurationRepository") {
    InMapDbJobConfigurationRepository.clear()
    InMapDbJobConfigurationRepository.add("1", jobConfiguration)
    val _collection = InMapDbJobConfigurationRepository.getAll
    val getjobConfiguration = _collection.get("1")
    assert( getjobConfiguration.get.name == jobConfiguration.name
            && _collection.size == InMapDbJobConfigurationRepository.size )
  }

  //InMemoryJobConfigurationRepository
  test("size InMemoryJobConfigurationRepository") {
    InMemoryJobConfigurationRepository.clear()
    InMemoryJobConfigurationRepository.add("1", jobConfiguration)
    InMemoryJobConfigurationRepository.add("2", jobConfiguration)
    InMemoryJobConfigurationRepository.add("3", jobConfiguration)
    assert(InMemoryJobConfigurationRepository.size == 3)
  }

  test("Clear InMemoryJobConfigurationRepository") {
    InMemoryJobConfigurationRepository.clear()
    assert(InMemoryJobConfigurationRepository.size == 0)
  }

  test("Add in InMemoryJobConfigurationRepository") {
    InMemoryJobConfigurationRepository.clear()
    InMemoryJobConfigurationRepository.add("1", jobConfiguration)
    assert(InMemoryJobConfigurationRepository.size == 1)
  }

  test("Remove from InMemoryJobConfigurationRepository") {
    InMemoryJobConfigurationRepository.clear()
    InMemoryJobConfigurationRepository.add("1", jobConfiguration)
    InMemoryJobConfigurationRepository.remove("1")
    assert(InMemoryJobConfigurationRepository.size == 0)
  }

  test("get from InMemoryJobConfigurationRepository") {
    InMemoryJobConfigurationRepository.clear()
    InMemoryJobConfigurationRepository.add("1", jobConfiguration)
    assert( InMemoryJobConfigurationRepository.get("1").name == jobConfiguration.name)
  }

  test("getAll from InMemoryJobConfigurationRepository") {
    InMemoryJobConfigurationRepository.clear()
    InMemoryJobConfigurationRepository.add("1", jobConfiguration)
    val _collection = InMemoryJobConfigurationRepository.getAll
    val getjobConfiguration = _collection.get("1")
    assert( getjobConfiguration.get.name == jobConfiguration.name
      && _collection.size == InMemoryJobConfigurationRepository.size )
  }

  override def afterAll(): Unit ={
    InMapDbJobConfigurationRepository.clear()
    InMemoryJobConfigurationRepository.clear()
  }
}

class JobTests extends FunSuite with Eventually with BeforeAndAfterAll with JsonFormatSupport with DefaultJsonProtocol {


  val jobConfiguration_Empty = new JobConfiguration(None, None, None, "Empty Test Jobconfiguration", Map().empty, Option("1"))
  val jobConfiguration_Python = new JobConfiguration(None, Option("some.py"), None, "Python Test Jobconfiguration", Map().empty, Option("2"))
  val jobConfiguration_Jar = new JobConfiguration(Option("some.jar"), None, Option("SomeClassName"), "Jar Test Jobconfiguration", Map().empty, Option("3"))
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

  test("FileNotFoundException") {
      intercept[java.lang.Exception] {
        val json = TestConfig.request_badpatch.parseJson
        val jobConfiguration = json.convertTo[JobConfiguration]
        val someJob = Job(jobConfiguration, contextWrapper, "Test Jar Job")
      }
  }

  test("Jar job") {
    val json = TestConfig.request_jar.parseJson
    val jobConfiguration = json.convertTo[JobConfiguration]
    val someJarJob = Job(jobConfiguration, contextWrapper, "Test Jar Job")
    eventually(timeout(30 seconds), interval(500 milliseconds)) {
      assert(someJarJob.status == JobStatus.Initialized)
    }
  }

  test("Jar job sql") {
    if(checkSparkSessionLogic)
      cancel("Can't run in Spark 2.0.0")
    val json = TestConfig.request_sparksql.parseJson
    val jobConfiguration = json.convertTo[JobConfiguration]
    val someJarJob = Job(jobConfiguration, contextWrapper, "Test Jar Job")
    eventually(timeout(20 seconds), interval(500 milliseconds)) {
      assert(someJarJob.status == JobStatus.Initialized)
    }
  }

  test("Jar job hive") {
    if(checkSparkSessionLogic)
      cancel("Can't run in Spark 2.0.0")
    val json = TestConfig.request_sparkhive.parseJson
    val jobConfiguration = json.convertTo[JobConfiguration]
    val someJarJob = Job(jobConfiguration, contextWrapper, "Test Jar Job")
    eventually(timeout(30 seconds), interval(500 milliseconds)) {
      assert(someJarJob.status == JobStatus.Initialized)
    }
  }

  test("Py job") {
    if(checkSparkSessionLogic)
      cancel("Can't run in Spark 2.0.0")
    val json = TestConfig.request_pyspark.parseJson
    val jobConfiguration = json.convertTo[JobConfiguration]
    val somePyJob = Job(jobConfiguration, contextWrapper, "Test Jar Job")
    eventually(timeout(30 seconds), interval(500 milliseconds)) {
      assert(somePyJob.status == JobStatus.Initialized)
    }
  }

  test("Py job sql") {
    if(checkSparkSessionLogic)
      cancel("Can't run in Spark 2.0.0")
    val json = TestConfig.request_pysparksql.parseJson
    val jobConfiguration = json.convertTo[JobConfiguration]
    val somePyJob = Job(jobConfiguration, contextWrapper, "Test Jar Job")
    eventually(timeout(30 seconds), interval(500 milliseconds)) {
      assert(somePyJob.status == JobStatus.Initialized)
    }
  }

  test("Py job hive") {
    if(checkSparkSessionLogic)
      cancel("Can't run in Spark 2.0.0")
    val json = TestConfig.request_pysparkhive.parseJson
    val jobConfiguration = json.convertTo[JobConfiguration]
    val somePyJob = Job(jobConfiguration, contextWrapper, "Test Jar Job")
    eventually(timeout(30 seconds), interval(500 milliseconds)) {
      assert(somePyJob.status == JobStatus.Initialized)
    }
  }

  test("Jar job run") {
    val json = TestConfig.request_jar.parseJson
    val jobConfiguration = json.convertTo[JobConfiguration]
    val someJarJob = Job(jobConfiguration, contextWrapper, "Test Jar Job")
    someJarJob.run()
    eventually(timeout(30 seconds), interval(500 milliseconds)) {
      assert(someJarJob.status == JobStatus.Running)
    }
  }

  test("Jar job sql run ") {
    if(checkSparkSessionLogic)
      cancel("Can't run in Spark 2.0.0")
    val json = TestConfig.request_sparksql.parseJson
    val jobConfiguration = json.convertTo[JobConfiguration]
    val someJarJob = Job(jobConfiguration, contextWrapper, "Test Jar Job")
    someJarJob.run()
    eventually(timeout(30 seconds), interval(500 milliseconds)) {
      assert(someJarJob.status == JobStatus.Running)
    }
  }

  test("Jar job hive run ") {
    if(checkSparkSessionLogic)
      cancel("Can't run in Spark 2.0.0")
    val json = TestConfig.request_sparkhive.parseJson
    val jobConfiguration = json.convertTo[JobConfiguration]
    val someJarJob = Job(jobConfiguration, contextWrapper, "Test Jar Job")
    someJarJob.run()
    eventually(timeout(30 seconds), interval(500 milliseconds)) {
      assert(someJarJob.status == JobStatus.Running)
    }
  }

  test("Py job run") {
    val json = TestConfig.request_pyspark.parseJson
    val jobConfiguration = json.convertTo[JobConfiguration]
    val somePyJob = Job(jobConfiguration, contextWrapper, "Test Jar Job")
    somePyJob.run()
    eventually(timeout(30 seconds), interval(500 milliseconds)) {
      assert(somePyJob.status == JobStatus.Running)
    }
  }

  test("Py job sql run") {
    if(checkSparkSessionLogic)
      cancel("Can't run in Spark 2.0.0")
    val json = TestConfig.request_pysparksql.parseJson
    val jobConfiguration = json.convertTo[JobConfiguration]
    val somePyJob = Job(jobConfiguration, contextWrapper, "Test Jar Job")
    somePyJob.run()
    eventually(timeout(30 seconds), interval(500 milliseconds)) {
      assert(somePyJob.status == JobStatus.Running)
    }
  }

  test("Py job hive run ") {
    if(checkSparkSessionLogic)
      cancel("Can't run in Spark 2.0.0")
    val json = TestConfig.request_pysparkhive.parseJson
    val jobConfiguration = json.convertTo[JobConfiguration]
    val somePyJob = Job(jobConfiguration, contextWrapper, "Test Jar Job")
    somePyJob.run()
    eventually(timeout(30 seconds), interval(500 milliseconds)) {
      assert(somePyJob.status == JobStatus.Running)
    }
  }

  test("Jar job testerror run") {
    val json = TestConfig.request_testerror.parseJson
    val jobConfiguration = json.convertTo[JobConfiguration]
    val someJarJob = Job(jobConfiguration, contextWrapper, "Test Jar Job")
    someJarJob.run()
    eventually(timeout(30 seconds), interval(500 milliseconds)) {
      assert(someJarJob.status == JobStatus.Aborted)
    }
  }

  test("Py job error ") {
    val json = TestConfig.request_pyerror.parseJson
    val jobConfiguration = json.convertTo[JobConfiguration]
    val somePyJob = Job(jobConfiguration, contextWrapper, "Test Jar Job")
    somePyJob.run()
    eventually(timeout(30 seconds), interval(500 milliseconds)) {
      assert(somePyJob.status == JobStatus.Aborted)
    }
  }


  override def afterAll(): Unit ={
    contextWrapper.stop()

    Thread.sleep(5000)
  }

}

