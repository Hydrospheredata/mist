package mist.api

import io.hydrosphere.mist.api.{RuntimeJobInfo, SetupConfiguration}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

class BaseContextsSpec extends FunSpec with Matchers with BeforeAndAfterAll {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("mist-lib-test")
    .set("spark.driver.allowMultipleContexts", "true")
    .set("spark.ui.disabled", "true")

  var sc: SparkContext = _

  override def beforeAll(): Unit = {
    sc = new SparkContext(conf)
  }

  override def afterAll = {
    sc.stop()
  }

  import JobDefInstances._
  import DefaultEncoders._
  import BaseContexts._

  it("for spark context") {
   val spJob = arg[Seq[Int]]("nums").onSparkContext(
     (nums: Seq[Int], sp: SparkContext) => {
       sp.parallelize(nums).map(_ * 2).collect()
       Map(1 -> "2")
    })
    val res = spJob.invoke(testCtx("nums" -> (1 to 10)))
    res shouldBe JobSuccess(Map(1 -> "2"))
  }

  it("for only sc") {
    val spJob: JobDef[Int] = onSparkContext((sc: SparkContext) => {
      5
    })
    val res = spJob.invoke(testCtx())
    res shouldBe JobSuccess(5)
  }

  def testCtx(params: (String, Any)*): JobContext = {
    val duration = org.apache.spark.streaming.Duration(10 * 1000)
    val setupConf = SetupConfiguration(sc, duration, RuntimeJobInfo("test", "worker"), None)
    JobContext(setupConf, params.toMap)
  }
}
