import java.util

import org.apache.spark.ml.feature.{VectorSlicer, Word2Vec, Word2VecModel}
import org.apache.spark.ml.linalg.Vector
import io.hydrosphere.mist.lib._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

object Word2VecJob extends MLMistJob with SQLSupport {
  def train(savePath: String): Map[String, Any] = {
    val documentDF = session.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val pipeline = new Pipeline().setStages(Array(word2Vec))

    val model = pipeline.fit(documentDF)

    model.write.overwrite().save(savePath)
    Map(
      "model" -> model.stages(0).asInstanceOf[Word2VecModel]
    )
  }

  def serve(modelPath: String, features: List[List[Double]]): Map[String, Any] = {
    import io.hydrosphere.mist.ml.LocalPipelineModel._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(
      LocalDataColumn("text", features)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.toMapList)
  }
}
