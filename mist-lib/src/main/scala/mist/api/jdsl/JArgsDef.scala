package mist.api.jdsl

import mist.api._
import mist.api.args.{ArgType, MInt, _}
import mist.api.data._
import mist.api.encoding.Encoder

case class RetVal[T](value: T, encoder: Encoder[T]) {
  def encoded(): JsLikeData = encoder(value)
}

trait RetVals {

  def fromAny[T](t: T): RetVal[T] = RetVal(t, new Encoder[T] {
    override def apply(a: T): JsLikeData = JsLikeData.fromJava(a)
  })

  def empty(): RetVal[Void] = RetVal(null, new Encoder[Void] {
    override def apply(a: Void): JsLikeData = JsLikeUnit
  })

}

object RetVals extends RetVals

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

  def intArg(name: String): ArgDef[jl.Integer] = arg[jl.Integer](name)(jInt)
  def intArg(name: String, defaultValue: jl.Integer): ArgDef[jl.Integer] = arg[jl.Integer](name, defaultValue)(jInt)

  def doubleArg(name: String): ArgDef[jl.Double] = arg[jl.Double](name)(jDouble)
  def doubleArg(name: String, defaultValue: jl.Double): ArgDef[jl.Double] = arg[jl.Double](name, defaultValue)(jDouble)

  def stringArg(name: String): ArgDef[String] = arg[String](name)(forString)
  def stringArg(name: String, defaultValue: String): ArgDef[String] = arg[String](name, defaultValue)(forString)

  private def createOptArg[T](name: String)(implicit desc: ArgDescription[T]): ArgDef[ju.Optional[T]] = {
    new ArgDef[ju.Optional[T]] {
      override def describe() = Seq(UserInputArgument(name, MOption(desc.`type`)))

      override def extract(ctx: JobContext) = {
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
  def optInt(name: String): ArgDef[ju.Optional[jl.Integer]] = createOptArg(name)(jInt)
  def optDouble(name: String): ArgDef[ju.Optional[jl.Double]] = createOptArg(name)(jDouble)
  def optString(name: String): ArgDef[ju.Optional[String]] = createOptArg(name)(forString)

  private def createListArg[T](name: String)(implicit desc: ArgDescription[T]) = new ArgDef[ju.List[T]]{
    override def describe() = Seq(UserInputArgument(name, MList(desc.`type`)))

    override def extract(ctx: JobContext) = ctx.params.get(name) match {
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
  def intList(name: String): ArgDef[ju.List[jl.Integer]] = createListArg(name)
  def doubleList(name: String): ArgDef[ju.List[jl.Double]] = createListArg(name)
  def stringList(name: String): ArgDef[ju.List[jl.String]] = createListArg(name)

  val allArgs: ArgDef[ju.Map[String, Any]] = new ArgDef[ju.Map[String, Any]] {
    override def describe() = Seq(InternalArgument)

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

