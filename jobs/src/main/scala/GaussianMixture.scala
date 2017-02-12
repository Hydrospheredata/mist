import io.hydrosphere.mist.lib._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.GaussianMixture


object GaussianMixture extends MLMistJob with SQLSupport {
  def train(): Map[String, Any] = {
    val dataset = session.read.format("libsvm").load("jobs/data/mllib/sample_kmeans_data.txt")

    val gmm = new GaussianMixture().setK(2)

    val model = gmm.fit(dataset)

    model.write.overwrite().save("models/gaussian_mixture")
    Map.empty[String, Any]
  }

  def serve(text: List[String]): Map[String, Any] = {
    import io.hydrosphere.mist.ml.transformers.LocalTransformers._

    val pipeline = PipelineLoader.load("models/gaussian_mixture")
    val data = LocalData(
      LocalDataColumn("text", text)
    )
    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("text", "prediction").toMapList)
  }
}
