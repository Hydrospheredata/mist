package io.hydrosphere.mist

import java.nio.file.Paths

import io.hydrosphere.mist.master.models.FunctionConfig
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

case class EnvArgs(
  imageName: String,
  examplesJar: String
)

class InsideDockerSpec extends FunSpec with Matchers with BeforeAndAfterAll {

  var container: TestContainer = _

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
    container = TestContainer.run(
      DockerImage(envArgs.imageName),
      Map(2004 -> 2004),
      Map("/var/run/docker.sock" -> "/var/run/docker.sock")
    )
    super.beforeAll()
  }

  override def afterAll = {
    container.close()
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
