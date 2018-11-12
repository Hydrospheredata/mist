package io.hydrosphere.mist.master.execution.workers.starter

import org.scalatest.{FunSpec, Matchers}
import org.scalatest.prop.TableDrivenPropertyChecks._
import cats.syntax.option._

class WorkerJarPathSpec extends FunSpec with Matchers {

  val local = WorkerJarPath.Local("/mist/mist-worker.jar")
  val http = WorkerJarPath.Http("http://addr/v2/api/artifacts_internal/mist-worker.jar")

  val expected = Table[Option[String], Option[String], WorkerJarPath](
    ("master", "deployMode", "jarPath"),
    (None, None, local),
    ("yarn".some, "client".some, local),
    ("yarn".some, "cluster".some, local),
    ("spark://addr".some, "cluster".some, http),
    ("spark://addr".some, "client".some, local),
    ("k8s://addr".some, "cluster".some, http),
    ("k8s://addr".some, "client".some, http),
    ("mesos://addr".some, "client".some, local),
    ("mesos://addr".some, "cluster".some, http)
  )

  it("should build path to worker jar") {
    forAll(expected) { (master, mode, path) =>
      val conf = Map(
        WorkerJarPath.DeployModeKey -> mode,
        WorkerJarPath.SparkMasterKey -> master
      ).collect({case (k, Some(v)) => k -> v})

      WorkerJarPath.pathFor(conf, "/mist", "addr") shouldBe path
    }
  }
}
