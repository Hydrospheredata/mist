package io.hydrosphere.mist

import java.nio.file.{Files, Path, Paths}

import com.dimafeng.testcontainers.{Container, GenericContainer}
import org.junit.runner.Description
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.wait.Wait

import scala.io.Source

case class EnvArgs(
  sparkVersion: String,
  mistVersion: String,
  imageName: String,
  workspace: String,
  sbtPath: String
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
      sparkVersion = read("sparkVersion"),
      mistVersion = read("mistVersion"),
      imageName = read("imageName"),
      workspace = read("workspace"),
      sbtPath = sys.env.getOrElse("SBT_PATH", "sbt")
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

  def prepareProject(name: String): Path = {
    val path = Paths.get(getClass.getClassLoader.getResource(name).getPath)
    val target = Paths.get(envArgs.workspace, name)
    if (Files.exists(target)) {
      fs.clean(target)
    }

    fs.createDir(target)
    fs.copyDir(path, target)

    val buildFile = target.resolve("build.sbt")
    val updated = Source.fromFile(buildFile.toFile).getLines()
      .map(s => {
        s.replace("{{SPARK_VERSION}}", envArgs.sparkVersion)
          .replace("{{MIST_VERSION}}", envArgs.mistVersion)
      }).mkString("\n")

    Files.write(buildFile, updated.getBytes())
    target
  }

  describe("hello-mist project") {

    it("should deploy and invoke") {
      import scala.sys.process._
      val dir = prepareProject("hellomist")

      val build = Process(s"${envArgs.sbtPath} package", Some(dir.toFile)).!
      if (build != 0) {
        throw new RuntimeException("Build failed")
      }

      import scalaj.http._

      val jarPath = dir.resolve("target/scala-2.11/hello-mist_2.11-0.0.1.jar")
      val bytes = Files.readAllBytes(jarPath)

      val uploadJar = Http("http://localhost:2004/v2/api/artifacts")
        .postMulti(MultiPart("file", "hello-mist.jar", "application/octet-stream", bytes))
        .asString

      println(uploadJar.body)

      val endpoint = Http("http://localhost:2004/v2/api/endpoints")
        .postData(
          """{"name": "hello-mist",
             | "path": "hello-mist.jar",
             | "className": "HelloMist$",
             | "defaultContext": "default"} """.stripMargin)

      println(endpoint.asString.body)

      val interface = MistHttpInterface("localhost", 2004)
      val result = interface.runJob("hello-mist")
      assert(result.success)
    }
  }

}
