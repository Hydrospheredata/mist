package mist.api.jdsl

import java.util
import java.util.Optional

import mist.api._
import mist.api.args.{ArgType, MInt, _}
import mist.api.data._

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


trait JArgsDef extends ArgDescriptionInstances {

  import java.{lang => jl, util => ju}

  import mist.api.JobDefInstances._

  import scala.collection.JavaConverters._

  implicit val jInt = new ArgDescription[jl.Integer] {
    override def `type`: ArgType = MInt

    override def apply(a: Any): Option[jl.Integer] = a match {
      case i: Int => Some(new jl.Integer(i))
      case _ => None
    }
  }
  implicit val jDouble = new ArgDescription[jl.Double] {
    override def `type`: ArgType = MDouble
    override def apply(a: Any): Option[jl.Double] = a match {
      case d: jl.Double => Some(new jl.Double(d))
      case _ => None
    }
  }

  private def namedArg[A](name: String)(implicit d: ArgDescription[A]): JUserArg[A] =
    new JUserArg[A](new NamedArgDef[A](name))

  private def namedArg[A](name: String, default: A)(implicit d: ArgDescription[A]): JUserArg[A] =
    new JUserArg[A](new NamedArgWithDefault[A](name, default))

  def intArg(name: String): JUserArg[jl.Integer] = namedArg(name)
  def intArg(name: String, defaultValue: jl.Integer): JUserArg[jl.Integer] = namedArg(name, defaultValue)

  def doubleArg(name: String): JUserArg[jl.Double] = namedArg(name)
  def doubleArg(name: String, defaultValue: jl.Double): JUserArg[jl.Double] = namedArg(name)

  def stringArg(name: String): JUserArg[String] = namedArg(name)
  def stringArg(name: String, defaultValue: String): JUserArg[String] = namedArg(name, defaultValue)

  private def optArg[T](name: String)(implicit desc: ArgDescription[T]): JUserArg[ju.Optional[T]] = {
    val arg = new UserArg[ju.Optional[T]] {
      override def describe() = Seq(UserInputArgument(name, MOption(desc.`type`)))

      override def extract(ctx: JobContext): ArgExtraction[Optional[T]] = {
        ctx.params.get(name) match {
          case Some(x) => desc.apply(x) match {
            case Some(a) => Extracted(ju.Optional.of(a))
            case None    => Missing(s"invalid type of $name - $x")
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

  private def listArg[T](name: String)(implicit desc: ArgDescription[T]): JUserArg[ju.List[T]] = {
    val arg = new UserArg[ju.List[T]] {
      override def describe() = Seq(UserInputArgument(name, MList(desc.`type`)))

      override def extract(ctx: JobContext): ArgExtraction[util.List[T]] = ctx.params.get(name) match {
        case Some(x) => x match {
          case seq: Seq[_] =>
            val optL = seq.map(a => desc.apply(a))

            if (optL.exists(_.isEmpty)) {
              Missing(s"Invalid type of list $name values could not resolve to type ${desc.`type`}")
            } else Extracted(optL.map(_.get).asJava)
          case _ => Missing(s"Invalid type of list $name values could not resolve to type ${desc.`type`}")
        }
        case None => Missing(s"Argument $name could not be found in ctx params")
      }
    }
    new JUserArg[ju.List[T]](arg)
  }

  def intListArg(name: String): JUserArg[ju.List[jl.Integer]] = listArg(name)
  def doubleListArg(name: String): JUserArg[ju.List[jl.Double]] = listArg(name)
  def stringListArg(name: String): JUserArg[ju.List[jl.String]] = listArg(name)

  val allArgs: JArg[ju.Map[String, Any]] = {
    val arg = JobDefInstances.allArgs.map(_.asJava)
    new JArg[ju.Map[String, Any]] {
      override def asScala: ArgDef[ju.Map[String, Any]] = arg
    }
  }
}

object JArgsDef extends JArgsDef

class JJobDef[T](val jobDef: JobDef[RetVal[T]])

abstract class JMistJob[T] extends JArgsDef with JJobDefinition {

  def defineJob: JJobDef[T]

  final def execute(ctx: JobContext): JobResult[JsLikeData] = {
    defineJob.jobDef.invoke(ctx) match {
      case JobSuccess(v) => JobSuccess(v.encoded())
      case f: JobFailure[_] => f.asInstanceOf[JobFailure[JsLikeData]]
    }
  }
}

