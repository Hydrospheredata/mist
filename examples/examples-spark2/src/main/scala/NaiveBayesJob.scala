import io.hydrosphere.mist.api._
import io.hydrosphere.spark_ml_serving._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.{Vector => LVector}
import org.apache.spark.sql.SparkSession


object NaiveBayesJob extends MLMistJob {
  def session: SparkSession = SparkSession
    .builder()
    .appName(context.appName)
    .config(context.getConf)
    .getOrCreate()

  def execute(savePath: String): Map[String, Any] = {
    val df = session.createDataFrame(Seq(
      (Vectors.dense(4.0, 0.2, 3.0, 4.0, 5.0), 1.0),
      (Vectors.dense(3.0, 0.3, 1.0, 4.1, 5.0), 1.0),
      (Vectors.dense(2.0, 0.5, 3.2, 4.0, 5.0), 1.0),
      (Vectors.dense(5.0, 0.7, 1.5, 4.0, 5.0), 1.0),
      (Vectors.dense(1.0, 0.1, 7.0, 4.0, 5.0), 0.0),
      (Vectors.dense(8.0, 0.3, 5.0, 1.0, 7.0), 0.0)
    )).toDF("features", "label")

    val nb = new NaiveBayes()

    val pipeline = new Pipeline().setStages(Array(nb))

    val model = pipeline.fit(df)

    model.write.overwrite().save(savePath)
    Map.empty[String, Any]
  }

  def serve(modelPath: String, features: List[List[Double]]): Map[String, Any] = {
    import LocalPipelineModel._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(LocalDataColumn("features", features))

    val result = pipeline.transform(data)

    val response = result.select("probability", "rawPrediction", "prediction").toMapList.map(rowMap => {
      val mapped = rowMap("probability").asInstanceOf[LVector].toArray
      val one = rowMap + ("probability" -> mapped)

      val mapped2 = one("rawPrediction").asInstanceOf[LVector].toArray
      one + ("rawPrediction" -> mapped2)
    })
    Map("result" -> response)
  }
}
