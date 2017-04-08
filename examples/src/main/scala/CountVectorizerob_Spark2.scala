import java.util

import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import io.hydrosphere.mist.lib._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

object CountVectorizerJob extends MLMistJob with SQLSupport {
  def train(savePath: String): Map[String, Any] = {
    val df = session.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")

    // fit a CountVectorizerModel from the corpus
    val cv = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
    val pipeline = new Pipeline().setStages(Array(cv))

    val model = pipeline.fit(df)

    model.write.overwrite().save(savePath)
    Map(
      "model" -> model.stages(0).asInstanceOf[CountVectorizerModel]
    )
  }

  def serve(modelPath: String, features: List[List[String]]): Map[String, Any] = {
    import io.hydrosphere.mist.ml.LocalPipelineModel._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(
      LocalDataColumn("words", features)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.toMapList)
  }
}
