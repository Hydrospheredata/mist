import io.hydrosphere.mist.lib._

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.Pipeline


object TFIDFJob extends MLMistJob with SQLSupport {
  def train(savePath: String): Map[String, Any] = {

    val df = session.createDataFrame(Seq(
      (0, "Provectus rocks!"),
      (0, "Machine learning for masses!"),
      (1, "BigData is a hot topick right now")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, idf))

    val model = pipeline.fit(df)

    model.write.overwrite().save(savePath)
    Map.empty[String, Any]
  }

  def serve(modelPath: String, sentences: List[String]): Map[String, Any] = {
    import io.hydrosphere.mist.ml.LocalPipelineModel._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(
      LocalDataColumn("sentence", sentences)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("sentence", "features").toMapList)
  }
}


