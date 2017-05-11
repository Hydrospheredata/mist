package io.hydrosphere.mist.utils.json

import io.hydrosphere.mist.jobs._
import spray.json.{DeserializationException, JsString, JsValue, RootJsonFormat}

trait JobConfigurationJsonSerialization extends AnyJsonFormatSupport {
  
  implicit object ConfigurationActionSupport extends RootJsonFormat[Action] {
    override def write(obj: Action): JsValue = JsString(obj.toString)

    override def read(json: JsValue): Action = json match {
      case JsString(str) => Action(str)
      case _ => throw DeserializationException("JobConfiguration.Action must be a string")
    }
  }

}
