import DTreeClassificationJob.context
import io.hydrosphere.mist.api._
import io.hydrosphere.mist.api.ml._
import io.hydrosphere.mist.api.ml.{LocalData, LocalDataColumn, PipelineLoader}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.sql.SparkSession


object GaussianMixtureJob extends MLMistJob {
  def session: SparkSession = SparkSession
    .builder()
    .appName(context.appName)
    .config(context.getConf)
    .getOrCreate()

  def train(): Map[String, Any] = {
    val dataset = session.read.format("libsvm").load("jobs/data/mllib/sample_kmeans_data.txt")

    val gmm = new GaussianMixture().setK(2)

    val pipeline = new Pipeline().setStages(Array(gmm))

    val model = pipeline.fit(dataset)

    model.write.overwrite().save("models/gaussian_mixture")
    Map.empty[String, Any]
  }

  def serve(text: List[String]): Map[String, Any] = {
    import LocalPipelineModel._

    val pipeline = PipelineLoader.load("models/gaussian_mixture")
    val data = LocalData(
      LocalDataColumn("text", text)
    )
    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("text", "prediction").toMapList)
  }
}
