package mist.api.args

sealed trait ArgType
case object MTInt extends ArgType
case object MTString extends ArgType
case object MTDouble extends ArgType

case class MTList(v: ArgType) extends ArgType
case class MTMap(k: ArgType, v: ArgType) extends ArgType
case class MTOption(v: ArgType) extends ArgType

case object MTAny extends ArgType

object ArgType {
  import scala.reflect.runtime.universe._

  def fromType(t: Type): ArgType = {
    val ref = t.asInstanceOf[TypeRefApi]
    t.typeSymbol.fullName match {
      case "scala.Int" => MTInt
      case "scala.Double" => MTDouble
      case "java.lang.String" => MTString
      case "scala.Any" => MTAny
      case "scala.Option" => MTOption(fromType(ref.args.head))
      case "scala.Array" =>
        MTList(fromType(ref.args.head))
      case "scala.collection.immutable.List" =>
        MTList(fromType(ref.args.head))

      case "scala.collection.immutable.Map" =>
        val kv = ref.args.map(fromType)
        MTMap(kv.head, kv.last)

      case x => throw new IllegalArgumentException(s"Type $x is not supported")
    }
  }
}
