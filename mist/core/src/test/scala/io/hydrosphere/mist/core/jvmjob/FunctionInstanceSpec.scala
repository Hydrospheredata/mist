package io.hydrosphere.mist.core.jvmjob

import mist.api.args._
import mist.api.data._
import mist.api.internal.BaseFunctionInstance
import mist.api.{FullFnContext}
import io.hydrosphere.mist.api.{RuntimeJobInfo, SetupConfiguration}
import io.hydrosphere.mist.core.CommonData.Action
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.Duration

import scala.reflect.ClassTag

class FunctionInstanceSpec extends FunSpec with Matchers with BeforeAndAfterAll {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("test")
    .set("spark.driver.allowMultipleContexts", "true")
    .set("spark.ui.disabled", "true")

  var sc: SparkContext = _

  val jobInfo = RuntimeJobInfo("test", "test")

  def jobContext(params: (String, Any)*): FullFnContext =
    FullFnContext(SetupConfiguration(sc, Duration(10), jobInfo, None), params.toMap)

  override def beforeAll = {
    sc = new SparkContext(conf)
  }

  override def afterAll = {
    sc.stop()
  }

  it("should execute") {
    val instance = instanceFor[MultiplyJob.type](Action.Execute)
    //TODO:!!
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

  it("should have internal arguments according to job mixins") {
    val inst = instanceFor[AllMixinsJob.type](Action.Execute)
    val internalArgs = inst.describe().collect { case x: InternalArgument => x }
    internalArgs should contain allElementsOf Seq(
      InternalArgument(Seq("streaming")),
      InternalArgument(Seq("sql")),
      InternalArgument(Seq("sql", "hive"))
    )
  }

  def instanceFor[A](action: Action)(implicit tag: ClassTag[A]): BaseFunctionInstance = {
    val clz = tag.runtimeClass
    FunctionInstanceLoader.Common.loadFnInstance(clz.getCanonicalName, action).get
  }
}
