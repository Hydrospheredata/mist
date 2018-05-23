package mist.api

sealed trait ArgType
case object MBoolean extends ArgType {
  override def toString: String = "Boolean"
}
case object MInt extends ArgType {
  override def toString: String = "Int"
}
case object MString extends ArgType {
  override def toString: String = "String"
}
case object MDouble extends ArgType {
  override def toString: String = "Double"
}

final case class MList(v: ArgType) extends ArgType {
  override def toString: String = s"List[$v]"
}
final case class MOption(v: ArgType) extends ArgType {
  override def toString: String = s"Option[$v]"
}

sealed trait RootArgType extends ArgType
final case class MMap(k: ArgType, v: ArgType) extends RootArgType {
  override def toString: String = s"Map[$k, $v]"
}

final case class MObj(fields: Seq[(String, ArgType)]) extends RootArgType {
  override def toString: String = {
    val fs = fields.map({case (k, v) => s"$k=$v"}).mkString(",")
    s"Object {$fs}"
  }
}
object MObj {
  val empty = MObj(Seq.empty)
}

case object MAny extends ArgType {
  override def toString: String = s"Any"
}

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
