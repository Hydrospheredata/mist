package io.hydrosphere.mist.ml

import io.hydrosphere.mist.lib.LocalData
import org.apache.spark.ml.{PipelineModel, Transformer}

import scala.collection.JavaConversions._

object LocalPipelineModel extends LocalTypedTransformer[PipelineModel] {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val constructor = classOf[PipelineModel].getDeclaredConstructor(classOf[String], classOf[java.util.List[Transformer]])
    constructor.setAccessible(true)
    val stages: java.util.List[Transformer] = data("stages").asInstanceOf[List[Transformer]] 
    constructor.newInstance(metadata.uid, stages)
  }

  override def transformTyped(sparkTransformer: PipelineModel, localData: LocalData): LocalData = {
    import scala.language.implicitConversions
    import ModelConversions._

    sparkTransformer.stages.foldLeft(localData)( (localData: LocalData, transformer: Transformer) =>
      transformer.transform(transformer, localData)
    )
  }
}
