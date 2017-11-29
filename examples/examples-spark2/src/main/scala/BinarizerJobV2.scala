import mist.api._
import mist.api.encoding.DatasetEncoding._
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{Dataset, SparkSession}

object BinarizerJobTrainV2 extends MistFn[Unit]{

  override def handler = {
    arg[String]("savePath").onSparkSession((path: String, spark: SparkSession) => {

      val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
      val dataFrame = spark.createDataFrame(data).toDF("id", "feature")

      val binarizer: Binarizer = new Binarizer()
        .setInputCol("feature")
        .setOutputCol("binarized_feature")
        .setThreshold(5.0)

      val pipeline = new Pipeline().setStages(Array(binarizer))

      val model = pipeline.fit(dataFrame)
      model.write.overwrite().save(path)
    })
  }
}

object BinarizerJobServeV2 extends MistFn[Dataset[_]]{

  override def handler = {(
    arg[String]("savePath") &
    arg[Seq[Double]]("features"))
    .onSparkSession((path: String, features: Seq[Double], spark: SparkSession) => {
      val x = features.map(v => Tuple1(v))
      val df = spark.createDataFrame(x).toDF("feature")
      val pipeline = PipelineModel.load(path)
      pipeline.transform(df)
    })
  }
}

