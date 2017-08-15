package io.hydrosphere.mist.jobs.jar

import io.hydrosphere.mist.api.{RuntimeJobInfo, SetupConfiguration}
import io.hydrosphere.mist.jobs.Action
import org.apache.spark.streaming.Duration
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

import scala.reflect.ClassTag

class JobInstanceSpec extends FunSpec with Matchers with BeforeAndAfterAll {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("test")
    .set("spark.driver.allowMultipleContexts", "true")

  var sc: SparkContext = _

  val jobInfo = RuntimeJobInfo("test", "test")
  def setupConf = SetupConfiguration(sc, Duration(10), jobInfo, None)

  override def beforeAll = {
    sc = new SparkContext(conf)
  }

  override def afterAll = {
    sc.stop()
  }

  it("should execute") {
    val instance = instanceFor[MultiplyJob.type](Action.Execute)

    instance.argumentsTypes shouldBe Map("numbers" -> MList(MInt))
    // valid params
    instance.run(setupConf, Map("numbers" -> List(1,2,4))) shouldBe Right(Map("r" -> List(2,4,8)))
    // invalid params
    instance.run(setupConf, Map.empty).isLeft shouldBe true
  }

  it("should apply optional params correctly") {
    val instance = instanceFor[OptParamJob.type](Action.Execute)

    instance.argumentsTypes shouldBe Map("p" -> MOption(MInt))

    instance.run(setupConf, Map("p" -> 1)) shouldBe Right(Map("r" -> 1))
    instance.run(setupConf, Map.empty) shouldBe Right(Map("r" -> 42))
  }

  // issue #198
  it("should apply arguments in correct order") {
    val instance = instanceFor[ManyArgJob.type](Action.Execute)

    val args = Map(
      "FromDate" -> "FromDate",
      "ToDate" -> "ToDate",
      "query" -> "query",
      "rows" -> 1,
      "Separator" -> "Separator"
    )

    instance.run(setupConf, args) shouldBe Right(Map("isOk" -> true))
  }

  def instanceFor[A](action: Action)(implicit tag: ClassTag[A]): JobInstance = {
    val clz = tag.runtimeClass
    JobsLoader.Common.loadJobInstance(clz.getCanonicalName, action).get
  }
}
