package io.hydrosphere.mist.utils.json

import io.hydrosphere.mist.master.WorkerLink
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

trait WorkerLinkJsonSerialization extends DefaultJsonProtocol {

  implicit val workerLinkJsonSerialization: RootJsonFormat[WorkerLink] = jsonFormat4(WorkerLink)
  
}
