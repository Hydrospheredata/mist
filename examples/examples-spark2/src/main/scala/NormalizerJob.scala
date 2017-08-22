import io.hydrosphere.mist.api._
import io.hydrosphere.spark_ml_serving._
import org.apache.spark.ml.linalg.{Vector => LVector}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object NormalizerJob extends MLMistJob{
  def session: SparkSession = SparkSession
    .builder()
    .appName(context.appName)
    .config(context.getConf)
    .getOrCreate()

  def execute(savePath: String): Map[String, Any] = {
    val df = session.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.5, -1.0)),
      (1, Vectors.dense(2.0, 1.0, 1.0)),
      (2, Vectors.dense(4.0, 10.0, 2.0))
    )).toDF("id", "features")

    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)

    val pipeline = new Pipeline().setStages(Array(normalizer))

    val model = pipeline.fit(df)

    model.write.overwrite().save(savePath)
    Map.empty[String, Any]
  }

  def serve(modelPath: String, features: List[List[Double]]): Map[String, Any] = {
    import LocalPipelineModel._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(LocalDataColumn("features", features))

    val response = pipeline.transform(data).toMapList.map(rowMap => {
      val conv = rowMap("normFeatures").asInstanceOf[LVector].toArray
      rowMap + ("normFeatures" -> conv)
    })


    Map("result" -> response)
  }
}
