package io.hydrosphere.mist.master.execution

import java.nio.file.Paths

import akka.actor.ActorSystem
import io.hydrosphere.mist.master.execution.aws.AWSEMRClusterRunner
import io.hydrosphere.mist.master.{AWSEMRLaunchSettings, LauncherSettings}
import io.hydrosphere.mist.master.models.{AWSEMRLaunchData, ContextConfig, ServerDefault}
import io.hydrosphere.mist.utils.akka.ActorRegHub

import scala.concurrent.Future

trait ClustersService {

  def start(id: String, ctx: ContextConfig): Future[Cluster]

}

object ClustersService {

  def create(
    mistHome: String,
    spawn: SpawnSettings,
    launchSettings: Map[String, LauncherSettings],
    serverDefault: ClusterRunner,
    system: ActorSystem
  ): ClustersService = {

    val regHub = ActorRegHub("regHub", system)
    val runners = launchSettings.map({case (name, settings) => {
      val runner = settings match {
        case aws: AWSEMRLaunchSettings =>
          AWSEMRClusterRunner.create(Paths.get(mistHome), spawn, aws, regHub, system)
      }
      name -> runner
    }})

    new ClustersService {

      override def start(id: String, ctx: ContextConfig): Future[Cluster] = {
        ctx.launchData match {
          case ServerDefault => serverDefault.run(id, ctx)
          case aws: AWSEMRLaunchData =>
            runners.get(aws.launcherSettingsName) match {
              case Some(runner) => runner.run(id, ctx)
              case None => Future.failed(new RuntimeException(s"Unknown settings name ${aws.launcherSettingsName} for ctx ${ctx.name}"))
            }
        }
      }
    }
  }

}
