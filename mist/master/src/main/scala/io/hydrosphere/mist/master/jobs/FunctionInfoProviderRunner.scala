package io.hydrosphere.mist.master.jobs

import akka.actor.{Actor, ActorRef, ActorSystem, Props, ReceiveTimeout}
import io.hydrosphere.mist.core.CommonData
import io.hydrosphere.mist.core.CommonData.RegisterJobInfoProvider
import io.hydrosphere.mist.master.FunctionInfoProviderConfig

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Future, Promise}
import scala.sys.process.Process

class FunctionInfoProviderRunner(
  runTimeout: FiniteDuration,
  cacheEntryTtl: FiniteDuration,
  masterHost: String,
  clusterPort: Int,
  sparkConf: Map[String, String]
) extends WithSparkConfArgs {

  def run()(implicit system: ActorSystem): Future[ActorRef] = {
    val refWaiter = ActorRefWaiter(runTimeout)(system)
    val cmd =
      Seq(s"${sys.env("MIST_HOME")}/bin/mist-function-info-provider",
        "--master", masterHost,
        "--cluster-port", clusterPort.toString,
        "--cache-entry-ttl", cacheEntryTtl.toMillis.toString)

    val builder = Process(cmd, None, ("SPARK_CONF", sparkConfArgs(sparkConf).mkString(" ")))
    builder.run(false)
    refWaiter.waitRef()
  }
}

trait WithSparkConfArgs {

  def sparkConfArgs(sparkConf: Map[String, String]): Seq[String] = {
    sparkConf.map { case (k, v) => s"--conf $k=$v" }
      .toSeq
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
        pr.failure(new IllegalStateException("Initialization of FunctionInfoProvider failed of timeout"))
        context stop self
    }
  }

  def apply(initTimeout: Duration)(implicit system: ActorSystem): ActorRefWaiter = new ActorRefWaiter {
    override def waitRef(): Future[ActorRef] = {
      val pr = Promise[ActorRef]
      system.actorOf(Props(new IdentityActor(pr, initTimeout)), CommonData.FunctionInfoProviderRegisterActorName)
      pr.future
    }
  }

}

object FunctionInfoProviderRunner {


  def create(config: FunctionInfoProviderConfig, masterHost: String, clusterPort: Int): FunctionInfoProviderRunner = {
    sys.env.get("SPARK_HOME") match {
      case Some(_) =>
        new FunctionInfoProviderRunner(config.runTimeout, config.cacheEntryTtl, masterHost, clusterPort, config.sparkConf)
      case None => throw new IllegalStateException("You should provide SPARK_HOME env variable for running mist")
    }

  }
}
