package io.hydrosphere.mist

import java.nio.file.{Files, Paths}

import org.scalatest.FunSpec

class TestUploadSpec extends FunSpec {

  it("asdasd") {
    import scalaj.http._

    val jar = "/home/dos65/projects/mist/examples/examples/target/scala-2.11/mist-examples_2.11-1.0.0-RC6.jar"
    val jarName = "docker-examples.jar"
    val endpointName = "spark-ctx"
    val clazz = "SparkContextExample$"

    val bytes = Files.readAllBytes(Paths.get(jar))
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

