import io.hydrosphere.mist.lib._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}


object DTree extends MLMistJob with SQLSupport {
  def train(): Map[String, Any] = {
    val data = session.read.format("libsvm").load("/data/mllib/sample_libsvm_data.txt")

    val Array(training, _) = data.randomSplit(Array(0.7, 0.3))

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)// features with > 4 distinct values are treated as continuous.
      .fit(data)
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    val model = pipeline.fit(training)

    model.write.overwrite().save("/models/dtree")
    Map.empty[String, Any]
}
  def serve(text: List[String]): Map[String, Any] = {
    import io.hydrosphere.mist.ml.transformers.LocalTransformers._

    val pipeline = PipelineLoader.load("/models/dtree")
    val data = LocalData(
      LocalDataColumn("text", text)
    )
    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("text", "prediction").toMapList)
  }
}
