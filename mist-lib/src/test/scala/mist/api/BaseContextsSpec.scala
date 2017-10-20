package mist.api

import io.hydrosphere.mist.api.{RuntimeJobInfo, SetupConfiguration}
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

class BaseContextsSpec extends FunSpec with Matchers with BeforeAndAfterAll {

  import BaseContexts._
  import JobDefInstances._

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

  it("for only hive context") {
    def pathToResource(path: String): String = {
      this.getClass.getClassLoader.getResource(path).getPath
    }

    val spJob = onHiveContext((hiveCtx: HiveContext) => {
      val df = hiveCtx.read.json(pathToResource("hive_job_data.json"))
      df.registerTempTable("temp")
      hiveCtx.sql("SELECT MAX(age) AS avg_age FROM temp")
        .take(1)(0).getLong(0)
    })

    val res = spJob.invoke(testCtx())
    res shouldBe JobSuccess(30)
  }

  def testCtx(params: (String, Any)*): JobContext = {
    val duration = org.apache.spark.streaming.Duration(10 * 1000)
    val setupConf = SetupConfiguration(TestSparkContext.sc, duration, RuntimeJobInfo("test", "worker"), None)
    JobContext(setupConf, params.toMap)
  }
}
