package io.hydrosphere.mist.common.logging

import io.hydrosphere.mist.common.CommonData.WorkerInitInfo

object AgentProtocol {

  case class Register(id: String)
  case class StartWorker(info: WorkerInitInfo)

}
