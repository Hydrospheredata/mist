import io.hydrosphere.mist.lib.spark2._
import io.hydrosphere.mist.lib.spark2.ml.LocalTransformers
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.PCA

import org.apache.spark.ml.linalg.Vectors

object PCAJob extends MLMistJob with SQLSupport {
  def train(savePath: String): Map[String, Any] = {
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

    model.write.overwrite().save(savePath)
    Map.empty[String, Any]
  }

  def serve(modelPath: String, features: List[Array[Double]]): Map[String, Any] = {
    import LocalTransformers._
    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(
      LocalDataColumn("features", features)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("features", "pcaFeatures").toMapList)
  }
}
