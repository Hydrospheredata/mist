package io.hydrosphere.mist.master.execution

import akka.actor.ActorSystem
import io.hydrosphere.mist.master.execution.workers.starter.WorkerStarter
import io.hydrosphere.mist.master.execution.workers.{PerJobConnection, WorkerConnection, WorkerRunner}
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.utils.akka.ActorRegHub

import scala.concurrent.Future

trait ClusterRunner {

  def run(id: String, ctx: ContextConfig): Future[Cluster]

}

object ClusterRunner {

  def legacy(
    spawn: SpawnSettings,
    regHub: ActorRegHub,
    workerStarter: WorkerStarter,
    system: ActorSystem
  ): ClusterRunner = {
    new ClusterRunner {
      val runner = WorkerRunner.default(spawn, workerStarter, regHub, system)
      override def run(id: String, ctx: ContextConfig): Future[Cluster] = {
        Future.successful(Cluster.actorBased(id, ctx, runner, system))
      }
    }
  }

  def awsEmr(
    spawn: SpawnSettings,
    regHub: ActorRegHub,
    system: ActorSystem
  ): ClusterRunner = {
    ???
  }
}

