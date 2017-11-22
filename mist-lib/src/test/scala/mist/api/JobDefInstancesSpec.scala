package mist.api

import org.apache.spark.SparkContext
import org.scalatest._
import org.scalatest.prop.TableDrivenPropertyChecks._

class JobDefInstancesSpec extends FunSpec with Matchers {

  import JobDefInstances._

  it("for named arg") {
    val x = arg[Int]("abc")
    x.extract(testCtx("abc" -> 42)) shouldBe Extracted(42)
    x.extract(testCtx()) shouldBe a[Missing[_]]
    x.extract(testCtx("abc" -> "str")) shouldBe a[Missing[_]]
  }

  it("for opt named arg") {
    val x = optArg[Int]("opt")
    x.extract(testCtx("opt" -> 1)) shouldBe Extracted(Some(1))
    x.extract(testCtx()) shouldBe Extracted(None)
    x.extract(testCtx("opt" -> "str")) shouldBe a[Missing[_]]
  }

  it("for named arg with default value") {
    val x = arg[Int]("abc", -1)
    x.extract(testCtx("abc" -> 42)) shouldBe Extracted(42)
    x.extract(testCtx()) shouldBe Extracted(-1)
    x.extract(testCtx("abc" -> "str")) shouldBe a[Missing[_]]
  }

  it("for all args") {
    allArgs.extract(testCtx("a" -> "b", "x" -> 42)) shouldBe
      Extracted(Map("a" -> "b", "x" -> 42))

    allArgs.extract(testCtx()) shouldBe Extracted(Map.empty)
  }

  it("for spark context") {
    import BaseContexts._

    val spJob: JobDef[Array[Int]] = arg[Seq[Int]]("nums").onSparkContext(
      (nums: Seq[Int], sp: SparkContext) => {
        sp.parallelize(nums).map(_ * 2).collect()
    })
    spJob.invoke(testCtx("nums" -> (1 to 10)))
  }

  describe("basic types") {

    def miss: Missing[Nothing] = Missing("")

    val expected = Table[ArgDef[_], Seq[(String, Any)], ArgExtraction[_]](
      ("arg", "data", "expected"),
      (arg[Boolean]("b"),        Seq("b" -> true), Extracted(true)),
      (arg[Boolean]("b", false), Seq.empty,        Extracted(false)),
      (arg[Boolean]("b"),        Seq.empty,        miss),
      (optArg[Boolean]("b"),     Seq("b" -> true), Extracted(Some(true))),
      (optArg[Boolean]("b"),     Seq.empty,        Extracted(None)),

      (arg[Int]("n"),    Seq("n" -> 2),  Extracted(2)),
      (arg[Int]("n", 0), Seq.empty,      Extracted(0)),
      (arg[Int]("n"),    Seq.empty,      miss),
      (optArg[Int]("n"), Seq("n" -> 42), Extracted(Some(42))),
      (optArg[Int]("n"), Seq.empty,      Extracted(None)),

      (arg[String]("s"),          Seq("s" -> "value"), Extracted("value")),
      (arg[String]("s", "value"), Seq.empty,           Extracted("value")),
      (arg[String]("s"),          Seq.empty,           miss),
      (optArg[String]("s"),       Seq("s" -> "yoyo"),  Extracted(Some("yoyo"))),
      (optArg[String]("s"),       Seq.empty,           Extracted(None)),

      (arg[Double]("d"),      Seq("d" -> 2.4),  Extracted(2.4)),
      (arg[Double]("d", 2.2), Seq.empty,        Extracted(2.2)),
      (arg[Double]("d"),      Seq.empty,        miss),
      (optArg[Double]("d"),   Seq("d" -> 42.1), Extracted(Some(42.1))),
      (optArg[Double]("d"),   Seq.empty,        Extracted(None)),

      (arg[Seq[Int]]("ints"),       Seq("ints" -> Seq(1,2,3)), Extracted(Seq(1, 2, 3))),

      (arg[Seq[Double]]("doubles"), Seq("doubles" -> Seq(1.1,2.2,3.3)), Extracted(Seq(1.1, 2.2, 3.3))),

      (arg[Seq[Double]]("strings"), Seq("strings" -> Seq("a", "b", "c")), Extracted(Seq("a", "b", "c"))),

      (arg[Seq[Boolean]]("booleans"), Seq("booleans" -> Seq(true, false)), Extracted(Seq(true, false)))
    )

    it("should extract expected result") {
      forAll(expected) { (arg, params, expected) =>
        val ctx = JobContext(params.toMap)
        val result = arg.extract(ctx)
        (expected, result) match {
          case (extr: Extracted[_], res: Extracted[_]) => res shouldBe extr
          case (extr: Missing[_], res: Extracted[_]) => fail(s"for $arg got $res, expected $extr")
          case _ =>
        }
      }
    }
  }


  def testCtx(params: (String, Any)*): JobContext = {
    JobContext(params.toMap)
  }

}
