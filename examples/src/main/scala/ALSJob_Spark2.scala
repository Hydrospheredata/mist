import io.hydrosphere.mist.lib._

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS

object ALSJob extends MLMistJob with SQLSupport  {
  def train(savePath: String): Map[String, Any] = {
    val ratings = session.createDataFrame(Seq(
      (1,92,2.0),
      (1,93,1.0),
      (1,94,2.0),
      (1,96,1.0),
      (1,97,1.0),
      (2,4,3.0),
      (2,6,1.0),
      (2,8,5.0),
      (2,9,1.0),
      (3,88,4.0),
      (3,89,1.0),
      (3,91,1.0),
      (3,94,3.0),
      (4,1,1.0),
      (4,6,1.0),
      (4,8,1.0),
      (4,9,1.0),
      (5,90,4.0),
      (5,91,2.0),
      (5,95,2.0),
      (5,99,1.0),
      (6,0,1.0),
      (6,1,1.0),
      (6,2,3.0),
      (6,5,1.0),
      (6,6,1.0),
      (6,9,1.0),
      (7,96,1.0),
      (7,97,1.0),
      (7,98,1.0),
      (8,0,1.0),
      (8,2,4.0),
      (8,3,2.0),
      (9,94,2.0),
      (9,95,3.0),
      (9,97,2.0),
      (9,98,1.0),
      (10,0,3.0),
      (10,2,4.0),
      (10,4,3.0),
      (10,7,.01)
    )).toDF("userId", "movieId", "rating")

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val pipeline = new Pipeline().setStages(Array(als))

    val model = pipeline.fit(ratings)

    val predictions = model.transform(ratings)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)

    model.write.overwrite().save(savePath)
    Map( "RMSE" -> rmse)
  }

  def serve(modelPath: String, userCol: List[Int], itemCol: List[Int]): Map[String, Any] = {
    import io.hydrosphere.mist.ml.LocalPipelineModel._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(
      LocalDataColumn("userId", userCol),
      LocalDataColumn("movieId", itemCol)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("userId", "movieId", "prediction").toMapList)
  }
}
