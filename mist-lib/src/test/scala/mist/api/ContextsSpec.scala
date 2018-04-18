package mist.api

import mist.api.data.{JsLikeNumber, JsLikeString}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{FunSpec, Matchers}

import scala.util._

class ContextsSpec extends FunSpec with Matchers with TestSparkContext {

  import mist.api.args.ArgsInstances._
  import mist.api.encoding.DefaultEncoders._

  it("for spark context") {
   val spJob = arg[Seq[Int]]("nums").onSparkContext(
     (nums: Seq[Int], sp: SparkContext) => {
       sp.parallelize(nums).map(_ * 2).collect()
       "2"
    })
    val res = spJob.invoke(testCtx("nums" -> (1 to 10)))
    res shouldBe Success(JsLikeString("2"))
  }

  it("for only sc") {
    val spJob: Handle = onSparkContext((sc: SparkContext) => {
      5
    })
    val res = spJob.invoke(testCtx())
    res shouldBe Success(JsLikeNumber(5))
  }

  def pathToResource(path: String): String = {
    this.getClass.getClassLoader.getResource(path).getPath
  }

  it("session with hive") {
    val spJob = onSparkSessionWithHive((spark: SparkSession) => {
      val df = spark.read.json(pathToResource("hive_job_data.json"))
      df.createOrReplaceTempView("temp")
      df.cache()
      spark.sql("DROP TABLE IF EXISTS temp_hive")
      spark.table("temp").write.mode(SaveMode.Overwrite).saveAsTable("temp_hive")

      spark.sql("SELECT MAX(age) AS avg_age FROM temp_hive")
        .take(1)(0).getLong(0)
    })
    spJob.invoke(testCtx())
    val res = spJob.invoke(testCtx())
    res shouldBe Success(JsLikeNumber(30))
  }

  def testCtx(params: (String, Any)*): FnContext = {
    FnContext(spark, params.toMap)
  }

}
