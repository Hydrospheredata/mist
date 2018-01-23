package io.hydrosphere.mist

import java.nio.file.{Files, Path, Paths}

import com.dimafeng.testcontainers.{Container, GenericContainer}
import org.junit.runner.Description
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.wait.Wait

import scala.io.Source

case class EnvArgs(
  imageName: String,
  examplesJar: String
)

class InsideDockerSpec extends FunSpec with Matchers with BeforeAndAfterAll {

  var container: Container = _
  implicit private val suiteDescription = Description.createSuiteDescription(getClass)

  val envArgs = {
    def read(name: String): String = sys.props.get(name) match {
      case Some(v) => v
      case None => throw new RuntimeException(s"Property $name is not set")
    }
    EnvArgs(
      imageName = read("imageName"),
      examplesJar = read("examplesJar")
    )
  }

  override def beforeAll = {
    container = GenericContainer(
      imageName = envArgs.imageName,
      fixedExposedPorts = Map(2004 -> 2004),
      waitStrategy = Wait.forListeningPort(),
      command = Seq("mist"),
      volumes = Seq(("/var/run/docker.sock", "/var/run/docker.sock", BindMode.READ_ONLY))
    )
    container.starting()
    super.beforeAll()
  }

  override def afterAll = {
    container.finished()
    super.afterAll
  }

  describe("hello-mist project") {

    it("should deploy and invoke") {
      import scalaj.http._

      val jarName = "docker-examples.jar"
      val endpointName = "spark-ctx"
      val clazz = "SparkContextExample$"

      println(envArgs)
      val bytes = Files.readAllBytes(Paths.get(envArgs.examplesJar))
      println(bytes.length)
      try {
        val uploadJar = Http("http://localhost:2004/v2/api/artifacts")
          .postMulti(MultiPart("file", jarName, "application/octet-stream", bytes))
          .asString
      } catch {
        case e: Throwable =>
          println("WTF???")
          throw e
      }

      val endpoint = Http("http://localhost:2004/v2/api/endpoints")
        .postData(
          s"""{"name": "$endpointName",
             | "path": "$jarName",
             | "className": "$clazz",
             | "defaultContext": "default"}""".stripMargin)

      val interface = MistHttpInterface("localhost", 2004)
      val result = interface.runJob(endpointName, "numbers" -> Seq(1,2,3,4))
      assert(result.success)
    }
  }

}
