package io.hydrosphere.mist.master.execution.workers.starter

import java.nio.file.Paths

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo

class SparkSubmitBuilder(mistHome: String, sparkHome: String) extends PsUtil {

  private val submitPath = Paths.get(sparkHome, "bin", "spark-submit").toString

  def submitWorker(name: String, info: WorkerInitInfo): Seq[String] = {
    val conf = info.sparkConf.flatMap({case (k, v) => Seq("--conf", s"$k=$v")})

    val runOpts = parseArguments(info.runOptions)
    val workerJar = WorkerJarPath.pathFor(info.sparkConf, mistHome, info.masterHttpConf)

    Seq(submitPath) ++ runOpts ++ conf ++ Seq(
      "--class", "io.hydrosphere.mist.worker.Worker",
      workerJar.value,
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
