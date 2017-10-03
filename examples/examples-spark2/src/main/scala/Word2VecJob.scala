import java.util

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.mllib.linalg.{Vector => LVector}
import io.hydrosphere.mist.api._
import io.hydrosphere.spark_ml_serving._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession

object Word2VecJob extends MLMistJob {
  def session: SparkSession = SparkSession
    .builder()
    .appName(context.appName)
    .config(context.getConf)
    .getOrCreate()

  def execute(savePath: String): Map[String, Any] = {
    val documentDF = session.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val pipeline = new Pipeline().setStages(Array(word2Vec))

    val model = pipeline.fit(documentDF)

    model.write.overwrite().save(savePath)
    Map.empty
  }

  def serve(modelPath: String, features: List[String]): Map[String, Any] = {
    import LocalPipelineModel._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(LocalDataColumn("text", features))
    val result = pipeline.transform(data)

    val response = result.select("result").toMapList.map(rowMap => {
      val mapped = rowMap("result").asInstanceOf[LVector].toArray
      rowMap + ("result" -> mapped)
    })

    Map("result" -> response)
  }
}
