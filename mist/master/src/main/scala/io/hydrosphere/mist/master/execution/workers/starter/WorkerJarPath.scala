package io.hydrosphere.mist.master.execution.workers.starter

import java.nio.file.Paths

sealed trait WorkerJarPath{
  val value: String
}

object WorkerJarPath {

  final case class Local(value: String) extends WorkerJarPath
  final case class Http(value: String) extends WorkerJarPath

  val SparkMasterKey = "spark.master"
  val DeployModeKey = "spark.submit.deployMode"

  def pathFor(
    sparkConf: Map[String, String],
    mistHome: String,
    mistHttpAddr: String
  ): WorkerJarPath = {

    def mkLocal(mistHome: String): Local = Local(Paths.get(mistHome, "mist-worker.jar").toString)
    def mkHttp(httpAddr: String): Http = Http("http://" + httpAddr + "/v2/api/artifacts_internal/mist-worker.jar")

    val master = sparkConf.get(SparkMasterKey)
    val deployMode = sparkConf.get(DeployModeKey)

    (master, deployMode) match {
      case (Some(m), _) if m.startsWith("yarn") => mkLocal(mistHome)
      case (Some(m), _) if m.startsWith("k8s://") => mkHttp(mistHttpAddr)
      case (_, Some("cluster")) => mkHttp(mistHttpAddr)
      case (_, _) => mkLocal(mistHome)
    }
  }

}
