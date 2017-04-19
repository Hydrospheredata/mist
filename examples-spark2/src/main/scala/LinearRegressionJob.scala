import java.util

import org.apache.spark.ml.feature._
import io.hydrosphere.mist.lib.spark2._
import io.hydrosphere.mist.lib.spark2.ml._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}

object LinearRegressionJob extends MLMistJob with SQLSupport {
  def train(savePath: String, datasetPath: String): Map[String, Any] = {
    val df = session.read.format("libsvm")
      .load(datasetPath)

    // fit a CountVectorizerModel from the corpus
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    val pipeline = new Pipeline().setStages(Array(lr))

    val model = pipeline.fit(df)

    model.write.overwrite().save(savePath)
    Map(
      "model" -> model.stages(0).asInstanceOf[LinearRegressionModel]
    )
  }

  def serve(modelPath: String, features: List[List[Double]]): Map[String, Any] = {
    import LocalPipelineModel._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(
      LocalDataColumn("features", features.map(_.toArray).map(Vectors.dense))
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.toMapList)
  }
}
