package io.hydrosphere.mist.lib.spark2.ml.loaders

import io.hydrosphere.mist.lib.spark2.ml.Metadata
import org.apache.spark.ml.Transformer

import scala.reflect.runtime.universe

object TransformerFactory {

  import ModelConversions._
  
  def apply(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule(metadata.`class` + "$")
    val obj = runtimeMirror.reflectModule(module)
    val localModel: LocalModel = obj.instance
    localModel.localLoad(metadata, data)
  }
  
}
