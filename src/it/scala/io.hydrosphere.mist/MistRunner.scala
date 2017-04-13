package io.hydrosphere.mist

import org.scalatest._
import scala.sys.process._

trait MistRunner {

  private val sparkHome = sys.props.getOrElse("sparkHome", throw new RuntimeException("spark home not set"))
  private val jar = sys.props.getOrElse("mistJar", throw new RuntimeException("mistJar not set"))

  def runMist(configPath: String): Process = {

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

  def killMist(): Unit ={
    Process("./bin/mist stop", None, "SPARK_HOME" -> sparkHome).run(false).exitValue()
  }
}

object MistRunner extends MistRunner

trait MistItTest extends BeforeAndAfterAll with MistRunner { self: Suite =>

  val configPath: String
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

}

