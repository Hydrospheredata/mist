import io.hydrosphere.mist.lib._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors


object PCA extends MLMistJob with SQLSupport {
  def train(): Map[String, Any] = {

    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df = session.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)

    val pipeline = new Pipeline().setStages(Array(pca))

    val model = pipeline.fit(df)

    model.write.overwrite().save("models/pca")
    Map.empty[String, Any]
  }

  def serve(features: List[Double]): Map[String, Any] = {
    import io.hydrosphere.mist.ml.transformers.LocalTransformers._

    val features = List(
      Array(2.0, 0.0, 3.0, 4.0, 5.0),
      Array(4.0, 0.0, 0.0, 6.0, 7.0)
    )

    val pipeline = PipelineLoader.load("models/pca")
    val data = LocalData(
      LocalDataColumn("features", features)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("features", "pcaFeatures").toMapList)
  }
}
