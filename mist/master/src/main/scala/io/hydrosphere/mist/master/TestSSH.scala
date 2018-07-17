package io.hydrosphere.mist.master

import java.util.concurrent.Executors

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master.execution.workers.starter.SSHRunner

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object TestSSH extends App {

  runner.onStart(
    "mist-worker_yoyo",
    WorkerInitInfo(
      sparkConf = Map(
        "spark.executor.instances" -> "2",
       // "spark.submit.deployMode" -> "cluster",
        "spark.master" -> "yarn",
        "spark.executor.memory" -> "1G",
        "spark.executor.cores" -> "2"
      ),
      maxJobs = 1,
      downtime = Duration.Inf,
      streamingDuration = 10 seconds,
      logService = "172.31.40.215:2005",
      masterAddress = "172.31.40.215:2551",
      masterHttpConf = "172.31.40.215:2004",
      maxArtifactSize = 250L,
      runOptions = ""
    )
  )
}
