package mist.api.jdsl

import java.util
import java.util.Optional

import mist.api._
import mist.api.args.{ArgType, MInt, _}
import mist.api.data._

class JUserArg[A](underlying: UserArg[A]) {

  def validated(f: Func1[A, java.lang.Boolean], reason: String): JUserArg[A] =
    new JUserArg(underlying.validated((a: A) => f(a), reason))

  def validated(f: Func1[A, java.lang.Boolean]): JUserArg[A] =
    new JUserArg(underlying.validated((a: A) => f(a)))

  def map[B](f: Func1[A, B]): JUserArg[B] = {
    val x = underlying.map((a: A) => f(a))
    new JUserArg[B](underlying.map((a: A) => f(a)))
  }

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

  private def namedArg[A](name: String): JUserArg[A] =
    new NamedArgDef[A](name) with JUserArg[A]

  private def namedArg[A](name: String, default: A): JUserArg[A] =
    new NamedArgWithDefault[A](name, default) with JUserArg[A]

  private def optArg[A](name: String): JUserArg[ju.Optional[A]] = {
    val x = new OptionalNamedArgDef[A](name).map({
      case Some(v) => Optional.of(v)
      case None => Optional.empty()
    })

    val x = new OptionalNamedArgDef[A](name).map() with JUserArg[ju.Optional[A]]
  }

  def intArg(name: String): JUserArg[jl.Integer] = namedArg(name)
  def intArg(name: String, defaultValue: jl.Integer): JUserArg[jl.Integer] = namedArg(name, defaultValue)

  def doubleArg(name: String): JUserArg[jl.Double] = namedArg(name)
  def doubleArg(name: String, defaultValue: jl.Double): JUserArg[jl.Double] = namedArg(name)

  def stringArg(name: String): JUserArg[String] = namedArg(name)
  def stringArg(name: String, defaultValue: String): JUserArg[String] = namedArg(name, defaultValue)

  private def createOptArg[T](name: String)(implicit desc: ArgDescription[T]): UserArg[ju.Optional[T]] = {
    new UserArg[ju.Optional[T]] {
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
  }
  def optInt(name: String): UserArg[ju.Optional[jl.Integer]] = createOptArg(name)(jInt)
  def optDouble(name: String): UserArg[ju.Optional[jl.Double]] = createOptArg(name)(jDouble)
  def optString(name: String): UserArg[ju.Optional[String]] = createOptArg(name)(forString)

  private def createListArg[T](name: String)(implicit desc: ArgDescription[T]) = new UserArg[ju.List[T]]{
    override def describe() = Seq(UserInputArgument(name, MList(desc.`type`)))

    override def extract(ctx: JobContext): ArgExtraction[util.List[T]] = ctx.params.get(name) match {
      case Some(x) => x match {
        case a: ju.List[_] =>
          val optL = a.asScala
            .map(a => desc.apply(a))

          if (optL.exists(_.isEmpty)) {
            Missing(s"Invalid type of list $name values could not resolve to type ${desc.`type`}")
          } else Extracted(optL.map(_.get).asJava)
      }
      case None => Missing(s"Argument $name could not be found in ctx params")
    }
  }
  def intList(name: String): UserArg[ju.List[jl.Integer]] = createListArg(name)
  def doubleList(name: String): UserArg[ju.List[jl.Double]] = createListArg(name)
  def stringList(name: String): UserArg[ju.List[jl.String]] = createListArg(name)

  val allArgs: ArgDef[ju.Map[String, Any]] = new SystemArg[ju.Map[String, Any]] {
    override def extract(ctx: JobContext) = Extracted(ctx.params.asJava)
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

