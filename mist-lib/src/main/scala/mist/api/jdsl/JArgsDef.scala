package mist.api.jdsl

import java.util.Optional

import mist.api._
import mist.api.data.JsNull
import mist.api.encoding.JsExtractor

/**
  * Wrap UserArg to support validated method for java
  */
trait JArg[A] {

  def asScala: ArgDef[A]
}

class JUserArg[A](underlying: UserArg[A]) extends JArg[A] {

  def validated(f: Func1[A, java.lang.Boolean], reason: String): JUserArg[A] =
    new JUserArg(underlying.validated((a: A) => f(a), reason))

  def validated(f: Func1[A, java.lang.Boolean]): JUserArg[A] =
    new JUserArg(underlying.validated((a: A) => f(a)))

  def asScala: UserArg[A] = underlying
}


trait JArgsDef {

  import java.{lang => jl, util => ju}

  import mist.api.encoding.defaults._
  import mist.api.ArgsInstances._

  import scala.collection.JavaConverters._

  private def namedArg[A](name: String)(implicit d: JsExtractor[A]): JUserArg[A] =
    new JUserArg[A](arg[A](name))

  private def namedArg[A](name: String, default: A)(implicit d: JsExtractor[A]): JUserArg[A] =
    new JUserArg[A](arg[A](name, default))

  def intArg(name: String): JUserArg[jl.Integer] = namedArg[jl.Integer](name)
  def intArg(name: String, defaultValue: jl.Integer): JUserArg[jl.Integer] = namedArg(name, defaultValue)

  def doubleArg(name: String): JUserArg[jl.Double] = namedArg[jl.Double](name)
  def doubleArg(name: String, defaultValue: jl.Double): JUserArg[jl.Double] = namedArg(name, defaultValue)

  def stringArg(name: String): JUserArg[String] = namedArg(name)
  def stringArg(name: String, defaultValue: String): JUserArg[String] = namedArg(name, defaultValue)

  def booleanArg(name: String): JUserArg[jl.Boolean] = namedArg[jl.Boolean](name)
  def booleanArg(name: String, defaultValue: jl.Boolean): JUserArg[jl.Boolean] = namedArg(name, defaultValue)

  private def optArg[T](name: String)(implicit ext: JsExtractor[T]): JUserArg[ju.Optional[T]] = {
    val arg = new UserArg[ju.Optional[T]] {
      override def describe() = Seq(UserInputArgument(name, MOption(ext.`type`)))
      override def extract(ctx: FnContext): Extraction[Optional[T]] = {
        ctx.params.fieldValue(name) match {
          case JsNull => Extracted(ju.Optional.empty())
          case x => ext(x).map(a => ju.Optional.of(a))
        }
      }
    }
    new JUserArg[Optional[T]](arg)
  }

  def optIntArg(name: String): JUserArg[ju.Optional[jl.Integer]] = optArg[jl.Integer](name)
  def optDoubleArg(name: String): JUserArg[ju.Optional[jl.Double]] = optArg[jl.Double](name)
  def optStringArg(name: String): JUserArg[ju.Optional[String]] = optArg[jl.String](name)
  def optBooleanArg(name: String): JUserArg[ju.Optional[jl.Boolean]] = optArg[jl.Boolean](name)

  private def listArg[T](name: String)(implicit ext: JsExtractor[Seq[T]]): JUserArg[ju.List[T]] = {
    import scala.collection.JavaConverters._
    val extL = ext.map(_.toList.asJava)
    val arg = new UserArg[ju.List[T]] {
      override def describe() = Seq(UserInputArgument(name, ext.`type`))
      override def extract(ctx: FnContext): Extraction[ju.List[T]] = extL(ctx.params.fieldValue(name))
    }
    new JUserArg[ju.List[T]](arg)
  }

  def intListArg(name: String): JUserArg[ju.List[jl.Integer]] = listArg(name)
  def doubleListArg(name: String): JUserArg[ju.List[jl.Double]] = listArg(name)
  def stringListArg(name: String): JUserArg[ju.List[jl.String]] = listArg(name)
  def booleanListArg(name: String): JUserArg[ju.List[jl.Boolean]] = listArg(name)

  val allArgs: JArg[ju.Map[String, Any]] = {
    val arg = ArgsInstances.allArgs.map(_.asJava)
    new JArg[ju.Map[String, Any]] {
      override def asScala: ArgDef[ju.Map[String, Any]] = arg
    }
  }
}

object JArgsDef extends JArgsDef
