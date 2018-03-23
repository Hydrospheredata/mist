package io.hydrosphere.mist.master.execution.workers.starter

import java.nio.file.Paths

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo

class SparkSubmitBuilder(mistHome: String, sparkHome: String) {

  private val localWorkerJar = Paths.get(mistHome, "mist-worker.jar").toString
  private val submitPath = Paths.get(sparkHome, "bin", "spark-submit").toString

  private def workerJarUrl(addr: String): String =
    "http://" + addr + "/v2/api/artifacts_internal/mist-worker.jar"

  def submitWorker(name: String, info: WorkerInitInfo): Seq[String] = {
    val conf = info.sparkConf.flatMap({case (k, v) => Seq("--conf", s"$k=$v")})

    val runOpts = {
      val trimmed = info.runOptions.trim
      if (trimmed.isEmpty) Seq.empty else trimmed.split(" ").map(_.trim).filter(_.nonEmpty).toSeq
    }

    val workerJar = if(info.isK8S) workerJarUrl(info.masterHttpConf) else localWorkerJar


    Seq(submitPath) ++ runOpts ++ conf ++ Seq(
      "--class", "io.hydrosphere.mist.worker.Worker",
      workerJar,
      "--master", info.masterAddress,
      "--name", name
    )
  }

}

object SparkSubmitBuilder {

  def apply(mistHome: String, sparkHome: String): SparkSubmitBuilder = {
    def realpath(p: String): String = Paths.get(p).toRealPath().toString
    new SparkSubmitBuilder(realpath(mistHome), realpath(sparkHome))
  }
}
