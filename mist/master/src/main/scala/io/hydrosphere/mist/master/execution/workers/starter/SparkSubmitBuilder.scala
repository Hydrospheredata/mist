package io.hydrosphere.mist.master.execution.workers.starter

import java.nio.file.Paths

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo

import scala.annotation.tailrec

class SparkSubmitBuilder(mistHome: String, sparkHome: String) {

  private val localWorkerJar = Paths.get(mistHome, "mist-worker.jar").toString
  private val submitPath = Paths.get(sparkHome, "bin", "spark-submit").toString

  private def workerJarUrl(addr: String): String =
    "http://" + addr + "/v2/api/artifacts_internal/mist-worker.jar"

  def submitWorker(name: String, info: WorkerInitInfo): Seq[String] = {
    val conf = info.sparkConf.flatMap({case (k, v) => Seq("--conf", s"$k=$v")})

    val runOpts = SparkSubmitBuilder.stringToArgs(info.runOptions)

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

  def stringToArgs(s: String): Seq[String] = {
    @tailrec
    def parse(in: String, curr: Vector[String]): Seq[String] = in.headOption match {
      case None => curr
      case Some(' ') => parse(in.dropWhile(_ == ' '), curr)
      case Some(sym) if sym == '"' || sym == ''' =>
        val (elem, tail) = in.tail.span(_ != sym)
        parse(tail.tail, curr :+ elem)
      case Some(_) =>
        val (elem, tail) = in.span(_ != ' ')
        parse(tail.tail, curr :+ elem)
    }
    parse(s, Vector.empty)
  }
}
