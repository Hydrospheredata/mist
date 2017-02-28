import io.hydrosphere.mist.lib._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.linalg.Vectors


object StandardScaler extends MLMistJob with SQLSupport {
  def train(): Map[String, Any] = {

    //val dataFrame = session.read.format("libsvm").load("jobs/data/mllib/sample_libsvm_data.txt")

    val data = Array(
      Vectors.dense(0.0, 10.3, 1.0, 4.0, 5.0),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df = session.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    val pipeline = new Pipeline().setStages(Array(scaler))

    val model = pipeline.fit(df)

    model.write.overwrite().save("models/standardscaler")
    Map.empty[String, Any]
  }

  def serve(features: List[Double]): Map[String, Any] = {
    import io.hydrosphere.mist.ml.transformers.LocalTransformers._

    val features = List(
      Array(2.0, 0.0, 3.0, 4.0, 5.0),
      Array(4.0, 0.0, 0.0, 6.0, 7.0)
    )

    val pipeline = PipelineLoader.load("models/standardscaler")
    val data = LocalData(
      LocalDataColumn("features", features)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("features", "scaledFeatures").toMapList)
  }
}
