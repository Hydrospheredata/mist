package io.hydrosphere.mist

import io.hydrosphere.mist.jobs.JobResult
import org.scalatest._

import scala.collection.immutable.IndexedSeq
import scalaj.http.Http

class Test01 extends FunSpec with MistItTest with Matchers {

  val configPath = "test01/integration.conf"

  import io.hydrosphere.mist.master.interfaces.http.JsonCodecs._
  import spray.json.pimpString

  it("should run py job") {
    val req = Http("http://localhost:2004/api/simple-context-py")
      .timeout(30 * 1000, 30 * 1000)
      .header("Content-Type", "application/json")
      .postData(
        s"""
           |{
           |  "numbers" : [1, 2, 3]
           |}
         """.stripMargin)

    val resp = req.asString
    resp.code shouldBe 200

    val result = resp.body.parseJson.convertTo[JobResult]
    result.success shouldBe true
  }
}


trait MistItTest extends BeforeAndAfterAll { self: Suite =>

  val configPath: String

  import scala.sys.process._

  private val sparkHome = sys.props.getOrElse("sparkHome", throw new RuntimeException("spark home not set"))
  private val jar = sys.props.getOrElse("mistJar", throw new RuntimeException("mistJar not set"))


  private var ps: Process = null

  override def beforeAll {
    ps = runMist(configPath)
  }

  override def afterAll: Unit = {
    // call kill over bash - destroy works strangely
    ps.destroy()
    killMist()
    Thread.sleep(1000)
  }

  private def runMist(configPath: String): Process = {

    val reallyConfigPath = getClass.getClassLoader.getResource(configPath).getPath
    val args = Seq(
      "./bin/mist", "start", "master",
      "--jar", jar,
      "--config", reallyConfigPath)

    val env = sys.env.toSeq :+ ("SPARK_HOME" -> sparkHome)
    val ps = Process(args, None, env: _*).run(new ProcessLogger {
      override def buffer[T](f: => T): T = f

      override def out(s: => String): Unit = ()

      override def err(s: => String): Unit = ()
    })
    Thread.sleep(5000)
    ps
  }

  private def killMist(): Unit ={
    Process("./bin/mist stop", None, "SPARK_HOME" -> sparkHome).run(false).exitValue()
  }


}

