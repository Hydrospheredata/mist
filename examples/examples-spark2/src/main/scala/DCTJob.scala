import BinarizerJob.context
import io.hydrosphere.mist.api._
import io.hydrosphere.spark_ml_serving._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.DCT
import org.apache.spark.ml.linalg.{Vector => LVector, Vectors => LVectors}
import org.apache.spark.sql.SparkSession

object DCTJob extends MLMistJob {
  def session: SparkSession = SparkSession
    .builder()
    .appName(context.appName)
    .config(context.getConf)
    .getOrCreate()

  def execute(savePath: String): Map[String, Any] = {
    val data = Seq(
      LVectors.dense(0.0, 1.0, -2.0, 3.0),
      LVectors.dense(-1.0, 2.0, 4.0, -7.0),
      LVectors.dense(14.0, -2.0, -5.0, 1.0)
    )
    val df = session.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val dct = new DCT()
      .setInputCol("features")
      .setOutputCol("featuresDCT")
      .setInverse(false)

    val pipeline = new Pipeline().setStages(Array(dct))

    val model = pipeline.fit(df)

    model.write.overwrite().save(savePath)
    Map.empty[String, Any]
  }

  def serve(modelPath: String, features: List[List[Double]]): Map[String, Any] = {
    import LocalPipelineModel._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(LocalDataColumn("features", features))
    val result = pipeline.transform(data)

    val response = result.select("category", "featuresDCT").toMapList.map(rowMap => {
      val mapped = rowMap("featuresDCT").asInstanceOf[LVector].toArray
      rowMap + ("featuresDCT" -> mapped)
    })

    Map("result" -> response)
  }
}
