import io.hydrosphere.mist.lib._

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.DCT
import org.apache.spark.ml.linalg.Vectors


object DCTJob extends MLMistJob with SQLSupport {
  def train(savePath: String): Map[String, Any] = {
    val data = Seq(
      Vectors.dense(0.0, 1.0, -2.0, 3.0),
      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
      Vectors.dense(14.0, -2.0, -5.0, 1.0)
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
    import io.hydrosphere.mist.ml.transformers.LocalTransformers._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(
      LocalDataColumn("features", features)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("features", "featuresDCT").toMapList)
  }
}
