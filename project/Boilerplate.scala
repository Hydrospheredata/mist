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


  val templates: Seq[Template] = List(
    GenHListerInstances, GenFnForTupleInstances,
    GenFuncInterfaces, GenFuncSyntax,
    GenJavaArgsClasses, GenJavaArgsMethods
  )

  /** Returns a seq of the generated files.  As a side-effect, it actually generates them... */
  def gen(dir : File) = for(t <- templates) yield {
    val tgtFile = dir / t.packageName / t.filename
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
  object GenHListerInstances extends ScalaTemplate {
    val filename = "HListerInstances.scala"
    def content(tv: TemplateVals): String = {
      import tv._
      block"""
         |package mist.api.internal
         |import shadedshapeless._
         |
         |trait HListerInstances extends LowerPriorityHLister {
         |
         -  implicit def hlister${arity}[${`A..N`}]: Aux[${`(A..N)`}, ${`A::N`}] = new HLister[${`(A..N)`}] {
         -    type Out = ${`A::N`}
         -    def apply(a: ${`(A..N)`}): ${`A::N`} = ${`(a._1::a._n)`}
         -  }
         |}
      """
    }
  }

  object GenFnForTupleInstances extends ScalaTemplate {
    val filename = "FnForTuple.scala"
    def content(tv: TemplateVals): String = {
      import tv._
      val FnIn = if (arity == 1) "A" else tv.`(A..N)`
      val Fn = s"$FnIn => Res"
      val fnApply = if (arity == 1) "f(in)" else "f.tupled(in)"
      block"""
         |package mist.api.internal
         |import scala.annotation.implicitNotFound
         |
         |trait FnForTuple[In, F] {
         |  type Out
         |  def apply(f: F, in: In): Out
         |}
         |trait FnForTupleInstances {
         |  @implicitNotFound("couldn't find FnForTuple for {$$F} instance. Ensure that your function is receiving the same parameters as declared in Arg")
         |  type Aux[In, F, Out0] = FnForTuple[In, F] { type Out = Out0 }
         -  implicit def fn${arity}[${`A..N`}, Res]: Aux[$FnIn, $Fn, Res] = new FnForTuple[$FnIn, $Fn] {
         -    type Out = Res
         -    def apply(f: $Fn, in: $FnIn): Res = $fnApply
         -  }
         |}
         |object FnForTuple extends FnForTupleInstances
      """
    }
  }

  object GenFuncInterfaces extends JavaTemplate {
    val filename = "functions.scala"

    def content(tv: TemplateVals) = {
      import tv._
      block"""
         |package mist.api.jdsl
         |
         -@FunctionalInterface
         -trait Func${arity}[${`-T1..N`}, +R] extends java.io.Serializable {
         -  @throws(classOf[Exception])
         -  def apply(${`a1:T1..aN:TN`}): R
         -}
         |
      """
    }
  }

  object GenFuncSyntax extends JavaTemplate {
    val filename = "functionsSyntax.scala"
    def content(tv: TemplateVals) = {
      import tv._
      block"""
         |package mist.api.jdsl
         |
         |trait FuncSyntax {
         |
         -  implicit class FuncSyntax${arity}[${`-T1..N`}, R](f: Func${arity}[${`T1..N`}, R]) {
         -    def toScalaFunc: Function${arity}[${`T1..N`}, R]= (${`a1:T1..aN:TN`}) => f.apply(${`a1..aN`})
         -  }
         |
         |}
         |object FuncSyntax extends FuncSyntax
      """
    }

  }


  object GenJavaArgsClasses extends JavaTemplate {
    val filename = "args.scala"
    override def range: Range.Inclusive = (2 to 21)
    def content(tv: TemplateVals) = {
      import tv._
      val extrasMethod = {
        if (arity != 21) {
          block"""
            -
            -  def withMistExtras(): Args${arity}[${`T1..N-1`}, MistExtras] =
            -    new Args${arity}[${`T1..N-1`}, MistExtras](${`a1..aN-1`}, MistExtras.mistExtras)
          """
        } else ""
      }
      block"""
         |package mist.api.jdsl
         |
         |import org.apache.spark.api.java.JavaSparkContext
         |import org.apache.spark.sql.SparkSession
         |import org.apache.spark.streaming.api.java.JavaStreamingContext
         |import mist.api._
         |import FuncSyntax._
         |import mist.api.SparkArgs._
         |import mist.api.ArgDef
         |
         -class Args${arity-1}[${`T1..N-1`}](${`ArgDef1..n-1`}){
         -
         -  /**
         -    * Define job execution that use JavaSparkContext for invocation
         -    */
         -  def onSparkContext[R](f: Func${arity}[${`T1..N-1`}, JavaSparkContext, R]): RawHandle[R] = {
         -    (${`a1&aN-1`} & javaSparkContextArg).apply(f.toScalaFunc)
         -  }
         -
         -  /**
         -    * Define job execution that use JavaStreamingContext for invocation
         -    */
         -  def onStreamingContext[R](f: Func${arity}[${`T1..N-1`}, JavaStreamingContext, R]): RawHandle[R] = {
         -    (${`a1&aN-1`} & javaStreamingContextArg).apply(f.toScalaFunc)
         -  }
         -
         -  def onSparkSession[R](f: Func${arity}[${`T1..N-1`}, SparkSession, R]): RawHandle[R] = {
         -    (${`a1&aN-1`} & sparkSessionArg).apply(f.toScalaFunc)
         -  }
         -
         -  def onSparkSessionWithHive[R](f: Func${arity}[${`T1..N-1`}, SparkSession, R]): RawHandle[R] = {
         -    (${`a1&aN-1`} & sparkSessionWithHiveArg).apply(f.toScalaFunc)
         -  }
         -
         -  def extract(ctx: FnContext): Extraction[${`(T1..N-1)`}] = (${`a1&aN-1`}).extract(ctx)
         -
         ${extrasMethod}
         -}
      """
    }
  }

  object GenJavaArgsMethods extends JavaTemplate {
    val filename = "WithArgs.scala"
    override def range: Range.Inclusive = (1 to 20)
    def content(tv: TemplateVals) = {
      import tv._
      block"""
         |package mist.api.jdsl
         |
         |import mist.api.ArgDef
         |
         |trait WithArgs {
         |
         -  /**
         -    * Declare ${arity} required arguments for job
         -    */
         -  def withArgs[${`T1..N`}](${`JArg1..n`}): Args${arity}[${`T1..N`}] =
         -    new Args${arity}(${`a1..aN_asScala`})
         |
         |}
         |
         |object WithArgs extends WithArgs
       """
    }
  }

  trait Template { self =>

    def packageName: String

    def createVals(arity: Int): TemplateVals = new TemplateVals(arity)

    def filename: String
    def content(tv: TemplateVals): String
    def range = 1 to 22
    def body: String = {
      val rawContents = range map { n => content(createVals(n)) split '\n' filterNot (_.isEmpty) }
      val preBody = rawContents.head takeWhile (_ startsWith "|") map (_.tail)
      val instances = rawContents flatMap {_ filter (_ startsWith "-") map (_.tail) }
      val postBody = rawContents.head dropWhile (_ startsWith "|") dropWhile (_ startsWith "-") map (_.tail)
      (preBody ++ instances ++ postBody) mkString "\n"
    }
  }

  trait ScalaTemplate extends Template {
    override def packageName = "mist/api/internal"
  }

  trait JavaTemplate extends Template {
    override def packageName = "mist/api/jdsl"
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
    val `(a._1::a._n)`  = ((1 to arity).map(i => s"a._$i") :+ "HNil").mkString("::")
    val `a:A..n:N`   = synTypedVals mkString ", "

    val synJavaTypes = (1 to arity) map (n => "T" + n)
    val synJavaVals  =  (1 to arity) map (n => "a" + n)
    val `T1..N`      = synJavaTypes.mkString(",")
    val `(T1..N)`    = if (arity == 1) "T1" else synJavaTypes.mkString("(", ", ", ")")
    val `(T1..N-1)`    = if (arity == 1) "T1" else synJavaTypes.dropRight(1).mkString("(", ", ", ")")
    val `T1..N-1`    = synJavaTypes.dropRight(1).mkString(",")
    val `-T1..N`     = synJavaTypes.map("-" + _).mkString(",")
    val `a1:T1..aN:TN`  = (synJavaVals zip synJavaTypes).map({case (a, t) => a + ":" + t}).mkString(",")
    val `a1..aN`     = synJavaVals.mkString(",")
    val `a1..aN_asScala` = synJavaVals.map(a => a + ".asScala").mkString(",")
    val `a1..aN-1`   = synJavaVals.dropRight(1).mkString(",")
    val `a1&aN`      = synJavaVals.mkString(" & ")
    val `a1&aN-1`    = synJavaVals.dropRight(1).mkString(" & ")

    val `ArgDef1..n` = {
      val types = synJavaTypes.map(t => s"ArgDef[$t]")
      (types zip synJavaVals).map({case (t, a) => a + ":" + t}).mkString(" ,")
    }
    val `JArg1..n` = {
      val types = synJavaTypes.map(t => s"JArg[$t]")
      (types zip synJavaVals).map({case (t, a) => a + ":" + t}).mkString(" ,")
    }
    val `ArgDef1..n-1` = {
      val types = synJavaTypes.map(t => s"ArgDef[$t]")
      (types zip synJavaVals).dropRight(1).map({case (t, a) => a + ":" + t}).mkString(" ,")
    }
  }

}