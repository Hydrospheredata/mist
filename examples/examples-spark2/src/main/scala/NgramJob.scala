import io.hydrosphere.mist.api._
import io.hydrosphere.spark_ml_serving._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SparkSession


object NgramJob extends MLMistJob{
  def session: SparkSession = SparkSession
    .builder()
    .appName(context.appName)
    .config(context.getConf)
    .getOrCreate()

  def execute(savePath: String): Map[String, Any] = {
    val df = session.createDataFrame(Seq(
      (0, Array("Provectus", "is", "such", "a", "cool", "company")),
      (1, Array("Big", "data", "rules", "the", "world")),
      (2, Array("Cloud", "solutions", "are", "our", "future"))
    )).toDF("id", "words")

    val ngram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams")

    val pipeline = new Pipeline().setStages(Array(ngram))

    val model = pipeline.fit(df)

    model.write.overwrite().save(savePath)
    Map.empty[String, Any]
  }

  def serve(modelPath: String, features: List[String]): Map[String, Any] = {

    import LocalPipelineModel._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(
      LocalDataColumn("words", List(features))
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("words", "ngrams").toMapList)
  }
}
