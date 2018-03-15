package io.hydrosphere.mist.master.execution.workers.starter

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.utils.Logger

class LocalSparkSubmit(builder: SparkSubmitBuilder) extends WorkerStarter with Logger {

  override def onStart(name: String, initInfo: WorkerInitInfo): WorkerProcess = {
    val cmd = builder.submitWorker(name, initInfo)
    logger.info(s"Try submit local worker $name, cmd: ${cmd.mkString(" ")}")
    val ps = WrappedProcess.run(cmd)
    val future = ps.await()
    Local(future)
  }

}

object LocalSparkSubmit {

  def apply(mistHome: String, sparkHome: String): LocalSparkSubmit =
    new LocalSparkSubmit(SparkSubmitBuilder(mistHome, sparkHome))

  def apply(): LocalSparkSubmit = (sys.env.get("MIST_HOME"), sys.env.get("SPARK_HOME")) match {
    case (Some(mHome), Some(spHome)) => LocalSparkSubmit(mHome, spHome)
    case (mist, spark) =>
      val errors = Seq(mist.map(_ => "MIST_HOME"),spark.map(_ => "SPARK_HOME")).mkString(",")
      val msg = s"Missing system environment variables: $errors"
      throw new IllegalStateException(msg)
  }
}

