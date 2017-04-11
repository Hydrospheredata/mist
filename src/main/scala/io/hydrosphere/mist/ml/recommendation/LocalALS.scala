package io.hydrosphere.mist.ml.recommendation

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, LocalTransformer, Metadata}
import org.apache.spark.ml.recommendation.{ALSModel, ALS}
import org.apache.spark.sql.DataFrame

class LocalALS(override val sparkTransformer: ALSModel) extends LocalTransformer[ALSModel] {
  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getItemCol) match {
      case Some(column) =>
        localData
      case None => localData
    }
  }
}

object LocalALS extends LocalModel[ALSModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): ALSModel = {
    // todo: solve dataframe issue and parsing parquete files
    val constructor = classOf[ALSModel].getDeclaredConstructor(
      classOf[String],
      classOf[Int],
      classOf[DataFrame],
      classOf[DataFrame]
    )
    constructor.setAccessible(true)

    constructor.newInstance()
  }

  override implicit def getTransformer(transformer: ALSModel): LocalTransformer[ALSModel] = new LocalALS(transformer)

}