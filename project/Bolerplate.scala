import sbt._

/**
  * Copied, with some modifications, from https://github.com/milessabin/shapeless/blob/master/project/Boilerplate.scala
  *
  * Generate a range of boilerplate classes, those offering alternatives with 0-22 params
  * and would be tedious to craft by hand
  */
object Boilerplate {

  import scala.StringContext._

  implicit class BlockHelper(val sc: StringContext) extends AnyVal {
    def block(args: Any*): String = {
      val interpolated = sc.standardInterpolator(treatEscapes, args)
      val rawLines = interpolated split '\n'
      val trimmedLines = rawLines map { _ dropWhile (_.isWhitespace) }
      trimmedLines mkString "\n"
    }
  }


  val templates: Seq[Template] = List(GenJobInstances)

  /** Returns a seq of the generated files.  As a side-effect, it actually generates them... */
  def gen(dir : File) = for(t <- templates) yield {
    val tgtFile = dir / "io" / "hydrosphere" / "mist" / "api" / "v2" / t.filename
    IO.write(tgtFile, t.body)
    tgtFile
  }

  /*
    Blocks in the templates below use a custom interpolator, combined with post-processing to produce the body

      - The contents of the `header` val is output first

      - Then the first block of lines beginning with '|'

      - Then the block of lines beginning with '-' is replicated once for each arity,
        with the `templateVals` already pre-populated with relevant relevant vals for that arity

      - Then the last block of lines prefixed with '|'

    The block otherwise behaves as a standard interpolated string with regards to variable substitution.
  */

  object GenJobInstances extends Template {
    val filename = "jobs.scala"
    override val range = 1 to 21
    def content(tv: TemplateVals) = {
      import tv._
      block"""
         |package io.hydrosphere.mist.api.v2
         |
         |import io.hydrosphere.mist.api.v2.hlist._
         |import org.apache.spark.SparkContext
         |
         |trait Jobs {
         |
         -  class JobArgs${arity}[${`A..N`}]
         -        (${`a:Arg[A]..n:Arg[N]`}) {
         -    def withContext[Out](func: (${`A..N`}, SparkContext) => JobResult[Out]): Job${arity}[${`A..N`}, Out] =
         -      new Job${arity}(${`a..n`}, func)
         -  }
         -  class Job${arity}[${`A..N`}, Out]
         -        (${`a:Arg[A]..n:Arg[N]`}, val func: (${`A..N`}, SparkContext) => JobResult[Out])
         -    extends JobP[Out] {
         -
         -    type RunArg = ${`A::N`}
         -    val args = ${`a::n`}
         -  }
         -
         -  def withArgs[${`A..N`}](${`a:Arg[A]..n:Arg[N]`}): JobArgs${arity}[${`A..N`}] =
         -    new JobArgs${arity}(${`a..n`})
         |}
      """
    }
  }

  trait Template {
    def filename: String
    def content(tv: TemplateVals): String
    def range = 1 to 22
    def body: String = {
      val rawContents = range map { n => content(new TemplateVals(n)) split '\n' filterNot (_.isEmpty) }
      val preBody = rawContents.head takeWhile (_ startsWith "|") map (_.tail)
      val instances = rawContents flatMap {_ filter (_ startsWith "-") map (_.tail) }
      val postBody = rawContents.head dropWhile (_ startsWith "|") dropWhile (_ startsWith "-") map (_.tail)
      (preBody ++ instances ++ postBody) mkString "\n"
    }
  }


  class TemplateVals(val arity: Int) {
    val synTypes     = (0 until arity) map (n => (n+'A').toChar)
    val synVals      = (0 until arity) map (n => (n+'a').toChar)
    val synTypedVals = (synVals zip synTypes) map { case (v,t) => v + ":" + t}

    val `A..N`       = synTypes.mkString(", ")
    val `A..N,Res`   = (synTypes :+ "Res") mkString ", "
    val `a..n`       = synVals.mkString(", ")
    val `A::N`       = (synTypes :+ "HNil") mkString "::"
    val `a::n`       = (synVals :+ "HNil") mkString "::"
    val `_.._`       = Seq.fill(arity)("_").mkString(", ")
    val `(A..N)`     = if (arity == 1) "Tuple1[A]" else synTypes.mkString("(", ", ", ")")
    val `(_.._)`     = if (arity == 1) "Tuple1[_]" else Seq.fill(arity)("_").mkString("(", ", ", ")")
    val `(a..n)`     = if (arity == 1) "Tuple1(a)" else synVals.mkString("(", ", ", ")")
    val `a:A..n:N`   = synTypedVals mkString ", "

    val synArgTypes = synTypes.map(t => s"Arg[$t]")
    val `Arg[A]..Arg[N]` = synArgTypes.mkString(", ")
    val `a:Arg[A]..n:Arg[N]` = (synVals zip synArgTypes) map { case (v, t) => v + ":" + t} mkString ", "
  }



}