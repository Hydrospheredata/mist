package io.hydrosphere.mist.ml

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.utils.ScalaUtils
import org.apache.spark.ml.{PipelineModel, Transformer}

import scala.collection.JavaConversions._

class LocalPipelineModel(override val sparkTransformer: PipelineModel) extends LocalTransformer[PipelineModel] {
  override def transform(localData: LocalData): LocalData = {
    import scala.language.implicitConversions
    import ModelConversions._

    sparkTransformer.stages.foldLeft(localData)( (localData: LocalData, transformer: Transformer) => {
      val model = ScalaUtils.companionOf(transformer.getClass)
      val localModel: LocalModel[Transformer] = model
      val localTransformer = localModel.getTransformer(transformer)
      localTransformer.transform(localData)
    })
  }
}

object LocalPipelineModel extends LocalModel[PipelineModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): PipelineModel = {
    val constructor = classOf[PipelineModel].getDeclaredConstructor(classOf[String], classOf[java.util.List[Transformer]])
    constructor.setAccessible(true)
    val stages: java.util.List[Transformer] = data("stages").asInstanceOf[List[Transformer]]
    constructor.newInstance(metadata.uid, stages)
  }

  implicit def getTransformer(transformer: PipelineModel): LocalPipelineModel = new LocalPipelineModel(transformer)
}
