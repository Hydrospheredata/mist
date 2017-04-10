package io.hydrosphere.mist.jobs.runners.jar

import io.hydrosphere.mist.api.SetupConfiguration
import io.hydrosphere.mist.jobs.Action
import org.apache.spark.streaming.Duration
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

import scala.reflect.ClassTag

class JobInstanceSpec extends FunSpec with Matchers with BeforeAndAfterAll {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("test")

  var sc: SparkContext = _

  override def beforeAll = {
    sc = new SparkContext(conf)
  }

  override def afterAll = {
    sc.stop()
  }

  it("should execute") {
    val instance = instanceFor[MultiplyJob.type](Action.Execute)
    val conf = new SetupConfiguration(sc, Duration(10), "", "")

    instance.argumentsTypes shouldBe Map("numbers" -> MList(MInt))
    // valid params
    instance.run(conf, Map("numbers" -> List(1,2,4))) shouldBe Right(Map("r" -> List(2,4,8)))
    // invalid params
    instance.run(conf, Map.empty).isLeft shouldBe true
  }

  it("should apply optional params correctly") {
    val instance = instanceFor[OptParamJob.type](Action.Execute)
    val conf = new SetupConfiguration(sc, Duration(10), "", "")

    instance.argumentsTypes shouldBe Map("p" -> MOption(MInt))

    instance.run(conf, Map("p" -> 1)) shouldBe Right(Map("r" -> 1))
    instance.run(conf, Map.empty) shouldBe Right(Map("r" -> 42))
  }

  def instanceFor[A](action: Action)(implicit tag: ClassTag[A]): JobInstance = {
    val clz = tag.runtimeClass
    JobsLoader.Common.loadJobInstance(clz.getCanonicalName, action).get
  }
}
