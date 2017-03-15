package io.hydrosphere.mist.lib.spark2.ml.parquet

import parquet.schema.{OriginalType, Type}

import scala.collection.immutable.{HashMap, Iterable}
import scala.collection.mutable.ListBuffer

case class NameValue(name: String, value: Any) {
  override def toString: String = {
    s"$name: ${value.toString}"
  }
}

class SimpleRecord {
  
  implicit class PrettyPrintMap[K, V](val map: Map[K, V]) {
    def prettyPrint: PrettyPrintMap[K, V] = this

    override def toString: String = {
      val valuesString = toStringLines.mkString("\n")

      "Map (\n" + valuesString + "\n)"
    }

    def toStringLines: Iterable[String] = {
      map
        .flatMap { case (k, v) => keyValueToString(k, v) }
        .map(indentLine)
    }

    def keyValueToString(key: K, value: V): Iterable[String] = {
      value match {
        case v: Map[_, _] => Iterable(key + " -> Map (") ++ v.prettyPrint.toStringLines ++ Iterable(")")
        case x => Iterable(key + " -> " + x.toString)
      }
    }

    def indentLine(line: String): String = {
      "\t" + line
    }
  }

  val values: scala.collection.mutable.ArrayBuffer[NameValue] = scala.collection.mutable.ArrayBuffer.empty[NameValue]

  def add(name: String, value: Any): Unit = {
    values.append(NameValue(name, value))
  }

  def struct(acc: HashMap[String, Any], schema: Type, parent: NameValue = null): HashMap[String, Any] = {
    var map = acc
    val eitherSchema = if (schema.isPrimitive) {
      Left(schema.asPrimitiveType())
    } else {
      Right(schema.asGroupType())
    }
    for (nameValue <- values) {
      val value = nameValue.value
      val name = nameValue.name

      eitherSchema match {
        case Left(_) => map += name -> value
        case Right(parentType) =>
          val subSchema = parentType.getType(name)
          if (subSchema.isPrimitive) {
            map += name -> value
          } else {
            parentType.getOriginalType match {
              case OriginalType.LIST =>
                val parentName = parent.name
                map += parentName -> value.asInstanceOf[SimpleRecord].struct(map.getOrElse(parentName, List.empty[Any]).asInstanceOf[List[Any]])
              case _ =>
                subSchema.asGroupType().getOriginalType match {
                  case OriginalType.LIST =>
                    map ++= value.asInstanceOf[SimpleRecord].struct(map.getOrElse(name, HashMap.empty[String, Any]).asInstanceOf[HashMap[String, Any]], subSchema, nameValue)
                  case _ =>
                    map += name -> value.asInstanceOf[SimpleRecord].struct(map.getOrElse(name, HashMap.empty[String, Any]).asInstanceOf[HashMap[String, Any]], subSchema, nameValue)
                }
            }
          }
      }
    }
    map
  }

  def struct(acc: List[Any]): List[Any] = {
    val list = acc.to[ListBuffer]
    for (nameValue <- values) {
      val value = nameValue.value
      val name = nameValue.name

      if (name.equals("element") && !classOf[SimpleRecord].isAssignableFrom(value.getClass)) {
        list += value
      }
    }
    list.to[List]
  }

  def prettyPrint(schema: Type): Unit = {
    println(struct(HashMap.empty[String, Any], schema).prettyPrint)
  }
}
