package io.hydrosphere.mist.core.jvmjob

import mist.api.args._
import mist.api.data._
import mist.api.internal.BaseJobInstance
import mist.api.{JobContext, FullJobContext}
import io.hydrosphere.mist.api.{RuntimeJobInfo, SetupConfiguration}
import io.hydrosphere.mist.core.CommonData.Action
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.Duration

import scala.reflect.ClassTag

class JobInstanceSpec extends FunSpec with Matchers with BeforeAndAfterAll {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("test")
    .set("spark.driver.allowMultipleContexts", "true")
    .set("spark.ui.disabled", "true")

  var sc: SparkContext = _

  val jobInfo = RuntimeJobInfo("test", "test")

  def jobContext(params: (String, Any)*): FullJobContext =
    FullJobContext(SetupConfiguration(sc, Duration(10), jobInfo, None), params.toMap)

  override def beforeAll = {
    sc = new SparkContext(conf)
  }

  override def afterAll = {
    sc.stop()
  }

  it("should execute") {
    val instance = instanceFor[MultiplyJob.type](Action.Execute)
    //TODO:!!
    //instance.argumentsTypes shouldBe Map("numbers" -> MList(MInt))
    // valid params
    instance.run(jobContext("numbers" -> List(1,2,4))) shouldBe
      Right(JsLikeMap("r" -> JsLikeList(Seq(2,4,8).map(i => JsLikeNumber(i)))))
    // invalid params
    instance.run(jobContext()).isLeft shouldBe true
  }

  it("should apply optional params correctly") {
    val instance = instanceFor[OptParamJob.type](Action.Execute)

    //TODO!!!
    //instance.argumentsTypes shouldBe Map("p" -> MOption(MInt))

    instance.run(jobContext("p" -> 1)) shouldBe Right(JsLikeMap("r" -> JsLikeNumber(1)))
    instance.run(jobContext()) shouldBe Right(JsLikeMap("r" -> JsLikeNumber(42)))
  }

  // issue #198
  it("should apply arguments in correct order") {
    val instance = instanceFor[ManyArgJob.type](Action.Execute)

    val args = Seq(
      "FromDate" -> "FromDate",
      "ToDate" -> "ToDate",
      "query" -> "query",
      "rows" -> 1,
      "Separator" -> "Separator"
    )

    instance.run(jobContext(args: _*)) shouldBe Right(JsLikeMap("isOk" -> JsLikeBoolean(true)))
  }

  def instanceFor[A](action: Action)(implicit tag: ClassTag[A]): BaseJobInstance = {
    val clz = tag.runtimeClass
    JobsLoader.Common.loadJobInstance(clz.getCanonicalName, action).get
  }
}
