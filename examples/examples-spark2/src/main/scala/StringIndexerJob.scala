import io.hydrosphere.mist.api._
import io.hydrosphere.spark_ml_serving._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession

object StringIndexerJob extends MLMistJob{
  def session: SparkSession = SparkSession
    .builder()
    .appName(context.appName)
    .config(context.getConf)
    .getOrCreate()


  def execute(savePath: String): Map[String, Any] = {
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
    import LocalPipelineModel._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(
      LocalDataColumn("category", features)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("category", "categoryIndex").toMapList)
  }
}
