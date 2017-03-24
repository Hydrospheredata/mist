import io.hydrosphere.mist.lib.spark2._
import io.hydrosphere.mist.lib.spark2.ml.LocalTransformers
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer

object StringIndexerJob extends MLMistJob with SQLSupport {

  def train(savePath: String): Map[String, Any] = {
    val df = session.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val pipeline = new Pipeline().setStages(Array(indexer))

    val model = pipeline.fit(df)

    model.write.overwrite().save(savePath)
    Map.empty[String, Any]
  }

  def serve(modelPath: String, features: List[String]): Map[String, Any] = {
    import LocalTransformers._
    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(
      LocalDataColumn("category", features)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("category", "categoryIndex").toMapList)
  }
}
