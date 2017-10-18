package io.hydrosphere.mist.master.jobs

import akka.actor.ActorRef
import io.hydrosphere.mist.core.CommonData.Action
import io.hydrosphere.mist.core.jvmjob.FullJobInfo
import io.hydrosphere.mist.master.data.EndpointsStorage

import scala.concurrent.Future

class JobInfoProviderService(
  jobInfoProvider: ActorRef,
  endpointStorage: EndpointsStorage
) {

  def getJobInfo(id: String): Future[Option[FullJobInfo]] = {
    ???
  }

  def validateJob(
    id: String,
    params: Map[String, Any],
    action: Action
  ): Future[Option[Boolean]] = {
    ???
  }

}
