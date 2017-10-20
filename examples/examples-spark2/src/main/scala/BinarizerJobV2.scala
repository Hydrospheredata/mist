import io.hydrosphere.mist.api.{RuntimeJobInfo, SetupConfiguration}
import mist.api._
import mist.api.encoding.DataSetEncoding._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.ml.{Pipeline, PipelineModel}

object BinarizerJobTrainV2 extends MistJob[Unit]{

  override def defineJob = {
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

object BinarizerJobServeV2 extends MistJob[Dataset[_]]{

  override def defineJob = {(
    arg[String]("savePath") &
    arg[Seq[Double]]("features"))
    .onSparkSession((path: String, features: Seq[Double], spark: SparkSession) => {
      import spark.implicits._
      val x = features.map(v => Tuple1(v))
      val df = spark.createDataFrame(x).toDF("feature")
      val pipeline = PipelineModel.load(path)
      pipeline.transform(df)
    })
  }
}

object TestBinarizer extends App {

  import org.apache.spark.streaming.Duration
  import mist.api.internal.JobInstance

  val sc = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    new SparkContext(conf)
  }
  val setupConf = SetupConfiguration(sc, Duration(60 * 1000), RuntimeJobInfo("test", "wtest"), None)

  val train = JobInstance.loadScala(Class.forName("BinarizerJobTrainV2$"))
  val res = train.run(JobContext(setupConf, Map("savePath" -> "./data")))

  println("Train done!")
  println(res)

  val serve = JobInstance.loadScala(Class.forName("BinarizerJobServeV2$"))
  val res2 = serve.run(JobContext(setupConf, Map("savePath" -> "./data", "features" -> Seq(0.1, 6.6))))

  println("Serve done!")
  println(res2)

  sc.stop()
}
