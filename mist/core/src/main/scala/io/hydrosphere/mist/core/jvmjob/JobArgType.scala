package io.hydrosphere.mist.core.jvmjob

//import scala.reflect.runtime.universe._
//
//sealed trait JobArgType
//
//case object MInt extends JobArgType
//case object MString extends JobArgType
//case object MDouble extends JobArgType
//case object MAny extends JobArgType
//
//case class MList(v: JobArgType) extends JobArgType
//case class MMap(k: JobArgType, v: JobArgType) extends JobArgType
//case class MOption(v: JobArgType) extends JobArgType
//
//object JobArgType {
//
//  def fromType(t: Type): JobArgType = {
//    val ref = t.asInstanceOf[TypeRefApi]
//    t.typeSymbol.fullName match {
//      case "scala.Int" => MInt
//      case "scala.Double" => MDouble
//      case "java.lang.String" => MString
//      case "scala.Any" => MAny
//      case "scala.Option" => MOption(fromType(ref.args.head))
//      case "scala.Array" =>
//        MList(fromType(ref.args.head))
//      case "scala.collection.immutable.List" =>
//        MList(fromType(ref.args.head))
//
//      case "scala.collection.immutable.Map" =>
//        val kv = ref.args.map(fromType)
//        MMap(kv.head, kv.last)
//
//      case x =>
//        throw new IllegalArgumentException(s"Type $x is not supported")
//    }
//  }
//}
