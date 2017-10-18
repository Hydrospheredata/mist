package io.hydrosphere.mist.master

import akka.actor.{Actor, ActorRef, ActorSystem, Props, ReceiveTimeout}
import io.hydrosphere.mist.core.CommonData
import io.hydrosphere.mist.core.CommonData.RegisterJobInfoProvider

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Future, Promise}
import scala.sys.process.Process

class JobInfoProviderRunner(
  runTimeout: FiniteDuration,
  masterHost: String,
  clusterPort: Int,
  httpPort: Int,
  sparkSubmitConf: Map[String, String]
) extends WithSparkSubmitArgs {

  def run()(implicit system: ActorSystem): Future[ActorRef] = {
    val refWaiter = ActorRefWaiter(runTimeout)(system)
    val cmd =
      Seq(s"${sys.env("MIST_HOME")}/bin/mist-job-info-provider",
        "--master", masterHost,
        "--cluster-port", clusterPort.toString,
        "--http-port", httpPort.toString) ++
        sparkSubmitArgs(sparkSubmitConf)

    val builder = Process(cmd)
    builder.run(false)
    refWaiter.waitRef()
  }
}

trait ActorRefWaiter {
  def waitRef(): Future[ActorRef]
}

object ActorRefWaiter {

  class IdentityActor(pr: Promise[ActorRef], initTimeout: Duration) extends Actor {

    override def preStart(): Unit = {
      context.setReceiveTimeout(initTimeout)
    }

    override def receive: Receive = {
      case RegisterJobInfoProvider(ref) =>
        pr.success(ref)
        context stop self

      case ReceiveTimeout =>
        pr.failure(new IllegalStateException("Initialization of JobInfoProvider failed of timeout"))
        context stop self
    }
  }

  def apply(initTimeout: Duration)(implicit system: ActorSystem): ActorRefWaiter = new ActorRefWaiter {
    override def waitRef(): Future[ActorRef] = {
      val pr = Promise[ActorRef]
      system.actorOf(Props(new IdentityActor(pr, initTimeout)), CommonData.JobExecutorRegisterActorName)
      pr.future
    }
  }

}

trait WithSparkSubmitArgs {

  def sparkSubmitArgs(sparkSubmitArgs: Map[String, String]): Seq[String] = {
    sparkSubmitArgs.map { case (k, v) => Seq("--" + k, v) }
      .toSeq
      .flatten
  }
}

object JobInfoProviderRunner {


  def create(config: JobInfoProviderConfig, masterHost: String, clusterPort: Int, httpPort: Int): JobInfoProviderRunner = {
    sys.env.get("SPARK_HOME") match {
      case Some(_) =>
        new JobInfoProviderRunner(config.runTimeout, masterHost, clusterPort, httpPort, config.sparkSubmitOpts)
      case None => throw new IllegalStateException("You should provide SPARK_HOME env variable for running mist")
    }

  }
}
