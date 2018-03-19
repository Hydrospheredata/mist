package io.hydrosphere.mist

import java.nio.file.Paths

import com.dimafeng.testcontainers.{Container, GenericContainer}
import io.hydrosphere.mist.master.models.FunctionConfig
import org.junit.runner.Description
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.wait.Wait

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
    // TODO: it looks that waiting strategy doesn't actually wait
    // TODO: update testcontainers - use java api directly - scala wrapper is ugly
    Thread.sleep(10000)
    super.beforeAll()
  }

  override def afterAll = {
    container.finished()
    super.afterAll
  }

  describe("hello-mist project") {

    it("should deploy and invoke") {
      val jarName = "docker-examples.jar"
      val functionName = "spark-ctx"
      val clazz = "SparkContextExample$"

      val interface = MistHttpInterface("localhost", 2004)
      interface.uploadArtifact(jarName, Paths.get(envArgs.examplesJar))
      interface.createFunction(
        FunctionConfig(
          name = functionName,
          path = jarName,
          className = clazz,
          defaultContext = "default"
        )
      )
      val result = interface.runJob(functionName, "numbers" -> Seq(1,2,3,4))
      assert(result.success)
    }
  }

}
