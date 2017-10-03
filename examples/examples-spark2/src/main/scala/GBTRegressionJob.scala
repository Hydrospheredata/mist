import GaussianMixtureJob.context
import io.hydrosphere.mist.api._
import io.hydrosphere.spark_ml_serving._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.sql.SparkSession

object GBTRegressionJob extends MLMistJob {
  def session: SparkSession = SparkSession
    .builder()
    .appName(context.appName)
    .config(context.getConf)
    .getOrCreate()

  def constructVector(params: Map[String, Any]): Vector = {
    Vectors.sparse(
      params("size").asInstanceOf[Int],
      params("indices").asInstanceOf[List[Int]].toArray[Int],
      params("values").asInstanceOf[List[Int]].map(_.toDouble).toArray[Double]
    )
  }

  def execute(savePath: String): Map[String, Any] = {
    val data = session.read.format("libsvm").load("examples/resources/sample_libsvm_data.txt")

    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(10)

    val pipeline = new Pipeline()
      .setStages(Array(gbt))

    val model = pipeline.fit(data)

    model.write.overwrite().save(savePath)
    Map.empty[String, Any]
  }

  def serve(modelPath: String, features: List[Map[String, Any]]): Map[String, Any] = {
    import LocalPipelineModel._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(LocalDataColumn("features", features.map(constructVector)))
    val result = pipeline.transform(data)

    val response = result.select("prediction").toMapList.map(rowMap => {
      val mapped = rowMap("prediction").asInstanceOf[Double]
      rowMap + ("prediction" -> mapped)
    })

    Map("result" -> response)
  }
}
