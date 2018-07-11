package io.hydrosphere.mist.master.execution.workers.starter

import com.decodified.scalassh._
import io.hydrosphere.mist.core.CommonData
import io.hydrosphere.mist.master.execution.workers.{StopAction, WorkerConnection, WorkerRunner, starter}
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class SSHRunner(
  user: String,
  key: String,
  host: String,
  ctx: ExecutionContext
) extends WorkerStarter with Logger {

  val cfgProvider = HostConfig(
    login = PublicKeyLogin(user, None, List(key)),
    hostName = host,
    port = 22,
    hostKeyVerifier = HostKeyVerifiers.DontVerify
  )
  val sparkSubmit = new SparkSubmit.HttpJarUrl("/usr/lib/spark")

  override def onStart(name: String, initInfo: CommonData.WorkerInitInfo): WorkerProcess = {
    val f = Future({
      val r = SSH(host, cfgProvider) { client =>
        client.exec(Command(sparkSubmit.command(name, initInfo).mkString(" ") + " &"))
      }
      r.get
    })(ctx)

    f.onComplete {
      case Success(result) =>
        logger.info(s"Worker $name was sucessfully submitted exitCode: ${result.exitCode}, stdout: ${result.stdOutAsString()} stderr: ${result.stdErrAsString()}")
      case Failure(e) =>
        logger.error(s"Submitting worker $name over ssh failed", e)
    }(ctx)
    WorkerProcess.NonLocal
  }

  override def stopAction: StopAction = StopAction.Remote

}
