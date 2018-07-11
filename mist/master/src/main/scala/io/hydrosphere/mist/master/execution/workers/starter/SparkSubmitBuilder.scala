package io.hydrosphere.mist.master.execution.workers.starter

import java.nio.file.Paths

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo

class SparkSubmitBuilder(mistHome: String, sparkHome: String) extends PsUtil {

  private val localWorkerJar = Paths.get(mistHome, "mist-worker.jar").toString
  private val submitPath = Paths.get(sparkHome, "bin", "spark-submit").toString

  private def workerJarUrl(addr: String): String =
    "http://" + addr + "/v2/api/artifacts_internal/mist-worker.jar"

  def submitWorker(name: String, info: WorkerInitInfo): Seq[String] = {
    val conf = info.sparkConf.flatMap({case (k, v) => Seq("--conf", s"$k=$v")})

    val runOpts = parseArguments(info.runOptions)

    val workerJar = if(info.isK8S) workerJarUrl(info.masterHttpConf) else localWorkerJar


    Seq(submitPath) ++ runOpts ++ conf ++ Seq(
      "--class", "io.hydrosphere.mist.worker.Worker",
      workerJar,
      "--master", info.masterAddress,
      "--name", name
    )
  }

}

sealed trait SparkSubmit extends PsUtil {
  def command(name: String, info: WorkerInitInfo): Seq[String]

  def buildCommand(
    name: String,
    info: WorkerInitInfo,
    workerJarPath: String,
    sparkSubmitPath: String
  ): Seq[String] = {
    val conf = info.sparkConf.flatMap({case (k, v) => Seq("--conf", s"$k=$v")})

    val runOpts = parseArguments(info.runOptions)

    Seq(sparkSubmitPath) ++ runOpts ++ conf ++ Seq(
      "--class", "io.hydrosphere.mist.worker.Worker",
      workerJarPath,
      "--master", info.masterAddress,
      "--name", name
    )
  }
}

object SparkSubmit {

  class LocalWorkerJar(workerJar: String, sparkSubmitPath: String) extends SparkSubmit {
    override def command(name: String, info: WorkerInitInfo): Seq[String] = {
      buildCommand(name, info, workerJar, sparkSubmitPath)
    }
  }

  class HttpJarUrl(sparkSubmitPath: String) extends SparkSubmit {
    private def workerJarUrl(addr: String): String = "http://" + addr + "/v2/api/artifacts_internal/mist-worker.jar"
    override def command(name: String, info: WorkerInitInfo): Seq[String] = {
      val url = workerJarUrl(info.masterHttpConf)
      buildCommand(name, info, url, sparkSubmitPath)
    }
  }

  class K8SAware(local: LocalWorkerJar, http: HttpJarUrl) extends SparkSubmit {
    override def command(name: String, info: WorkerInitInfo): Seq[String] = {
      val selected = if (info.isK8S) http else local
      selected.command(name, info)
    }
  }

  private def realpath(p: String): String = Paths.get(p).toRealPath().toString

  def local(mistHome: String, sparkHome: String): LocalWorkerJar = new LocalWorkerJar(realpath(mistHome), realpath(sparkHome))
  def http(sparkHome: String): HttpJarUrl = new HttpJarUrl(realpath(sparkHome))
  def k8sAware(mistHome: String, sparkHome: String): K8SAware = {
    new K8SAware(local(mistHome, sparkHome), http(sparkHome))
  }

}




object SparkSubmitBuilder {

  def apply(mistHome: String, sparkHome: String): SparkSubmitBuilder = {
    def realpath(p: String): String = Paths.get(p).toRealPath().toString
    new SparkSubmitBuilder(realpath(mistHome), realpath(sparkHome))
  }

}
