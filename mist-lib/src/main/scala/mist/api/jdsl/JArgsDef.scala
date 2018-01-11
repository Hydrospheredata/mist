package mist.api.jdsl

import java.util
import java.util.Optional

import mist.api._
import mist.api.args._

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

  import mist.api.args.ArgsInstances._

  import scala.collection.JavaConverters._

  private def namedArg[A](name: String)(implicit d: ArgExtractor[A]): JUserArg[A] =
    new JUserArg[A](arg[A](name))

  private def namedArg[A](name: String, default: A)(implicit d: ArgExtractor[A]): JUserArg[A] =
    new JUserArg[A](arg[A](name, default))

  def intArg(name: String): JUserArg[jl.Integer] = namedArg(name)
  def intArg(name: String, defaultValue: jl.Integer): JUserArg[jl.Integer] = namedArg(name, defaultValue)

  def doubleArg(name: String): JUserArg[jl.Double] = namedArg(name)
  def doubleArg(name: String, defaultValue: jl.Double): JUserArg[jl.Double] = namedArg(name, defaultValue)

  def stringArg(name: String): JUserArg[String] = namedArg(name)
  def stringArg(name: String, defaultValue: String): JUserArg[String] = namedArg(name, defaultValue)

  def booleanArg(name: String): JUserArg[jl.Boolean] = namedArg(name)
  def booleanArg(name: String, defaultValue: jl.Boolean): JUserArg[jl.Boolean] = namedArg(name, defaultValue)

  private def optArg[T](name: String)(implicit tP: ArgExtractor[T]): JUserArg[ju.Optional[T]] = {
    val arg = new UserArg[ju.Optional[T]] {
      override def describe() = Seq(UserInputArgument(name, MOption(tP.`type`)))

      override def extract(ctx: FnContext): ArgExtraction[Optional[T]] = {
        ctx.params.get(name) match {
          case Some(x) => tP.extract(x) match {
            case Extracted(a)  => Extracted(ju.Optional.of(a))
            case m: Missing[_] => m.asInstanceOf[Missing[Optional[T]]]
          }
          case None => Extracted(ju.Optional.empty())
        }
      }
    }
    new JUserArg[Optional[T]](arg)
  }

  def optIntArg(name: String): JUserArg[ju.Optional[jl.Integer]] = optArg(name)
  def optDoubleArg(name: String): JUserArg[ju.Optional[jl.Double]] = optArg(name)
  def optStringArg(name: String): JUserArg[ju.Optional[String]] = optArg(name)
  def optBooleanArg(name: String): JUserArg[ju.Optional[jl.Boolean]] = optArg(name)

  private def listArg[T](name: String)(implicit tP: ArgExtractor[T]): JUserArg[ju.List[T]] = {
    val arg = new UserArg[ju.List[T]] {
      override def describe() = Seq(UserInputArgument(name, MList(tP.`type`)))

      override def extract(ctx: FnContext): ArgExtraction[util.List[T]] = ctx.params.get(name) match {
        case Some(x) => x match {
          case seq: Seq[_] =>
            val elems = seq.map(a => tP.extract(a))

            if (elems.exists(_.isMissing)) {
              Missing(s"Invalid type of list $name values could not resolve to type ${tP.`type`}")
            } else Extracted(elems.collect({case Extracted(v) => v}).asJava)
          case _ => Missing(s"Invalid type of list $name values could not resolve to type ${tP.`type`}")
        }
        case None => Missing(s"Argument $name could not be found in ctx params")
      }
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
