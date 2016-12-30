package io.hydrosphere.mist.ml.loaders

import io.hydrosphere.mist.ml.Metadata
import org.apache.spark.ml.{PipelineModel, Transformer}

import scala.collection.JavaConversions._

object LocalPipelineModel extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val constructor = classOf[PipelineModel].getDeclaredConstructor(classOf[String], classOf[java.util.List[Transformer]])
    constructor.setAccessible(true)
    val stages: java.util.List[Transformer] = data("stages").asInstanceOf[List[Transformer]] 
    constructor.newInstance(metadata.uid, stages)
  }
}
