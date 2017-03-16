import io.hydrosphere.mist.lib._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StopWordsRemover


object StopWordsRemoverJob extends MLMistJob with SQLSupport  {
  def train(savePath: String): Map[String, Any] = {
    val df = session.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "balloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val pipeline = new Pipeline().setStages(Array(remover))

    val model = pipeline.fit(df)

    model.write.overwrite().save(savePath)
    Map.empty[String, Any]
  }

  def serve(modelPath: String, features: List[List[String]]): Map[String, Any] = {
    import io.hydrosphere.mist.ml.transformers.LocalTransformers._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(
      LocalDataColumn("raw", features)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("raw", "filtered").toMapList)
  }
}
