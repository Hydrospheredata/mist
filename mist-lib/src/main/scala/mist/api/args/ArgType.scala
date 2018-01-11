package mist.api.args

sealed trait ArgType
case object MBoolean extends ArgType
case object MInt extends ArgType
case object MString extends ArgType
case object MDouble extends ArgType

final case class MList(v: ArgType) extends ArgType
final case class MMap(k: ArgType, v: ArgType) extends ArgType
final case class MObj(fields: Seq[(String, ArgType)]) extends ArgType
final case class MOption(v: ArgType) extends ArgType

case object MAny extends ArgType

object ArgType {
  import scala.reflect.runtime.universe._

  def fromType(t: Type): ArgType = {
    val ref = t.asInstanceOf[TypeRefApi]
    t.typeSymbol.fullName match {
      case "scala.Int" => MInt
      case "scala.Double" => MDouble
      case "scala.Boolean" => MBoolean
      case "java.lang.String" => MString
      case "scala.Any" => MAny
      case "scala.Option" => MOption(fromType(ref.args.head))
      case "scala.Array" =>
        MList(fromType(ref.args.head))
      case "scala.collection.immutable.List" =>
        MList(fromType(ref.args.head))

      case "scala.collection.immutable.Map" =>
        val kv = ref.args.map(fromType)
        MMap(kv.head, kv.last)

      case x => throw new IllegalArgumentException(s"Type $x is not supported")
    }
  }
}
