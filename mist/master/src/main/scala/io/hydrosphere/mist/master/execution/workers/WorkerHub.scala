package io.hydrosphere.mist.master.execution.workers

import io.hydrosphere.mist.master.execution.SpawnSettings
import io.hydrosphere.mist.master.models.ContextConfig

case class MasterConnectionSettings(
  akkaAddress: String,
  logAddress: String,
  httpAddress: String
)

trait ConnectorStarter {

  def start(id: String, context: ContextConfig): WorkerConnector

}


class WorkerHub(
  connSettings: MasterConnectionSettings,
  spawnSettings: SpawnSettings
) extends ConnectorStarter with WorkersMirror  {

  override def start(id: String, context: ContextConfig): WorkerConnector = ???

}



