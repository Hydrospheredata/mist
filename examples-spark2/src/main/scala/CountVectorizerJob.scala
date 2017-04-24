import io.hydrosphere.mist.lib.spark2._
import io.hydrosphere.mist.lib.spark2.ml._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._

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

  def serve(modelPath: String, features: List[List[Double]]): Map[String, Any] = {
    import LocalPipelineModel._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(
      LocalDataColumn("words", features)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.toMapList)
  }
}
