import io.hydrosphere.mist.api._
import io.hydrosphere.spark_ml_serving._
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.linalg.{Vector => LVector}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession


object TFIDFJob extends MLMistJob {
  def session: SparkSession = SparkSession
    .builder()
    .appName(context.appName)
    .config(context.getConf)
    .getOrCreate()

  def execute(savePath: String): Map[String, Any] = {

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
    import LocalPipelineModel._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(LocalDataColumn("sentence", sentences))

    val result = pipeline.transform(data)
    val response = result.select("sentence", "features").toMapList.map(rowMap => {
      val conv = rowMap("features").asInstanceOf[LVector].toArray
      rowMap + ("features" -> conv)
    })
    Map("result" -> response)
  }
}


