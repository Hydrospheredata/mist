package io.hydrosphere.mist.api.ml.reader

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

  //TODO: rewrite parquet reader part totally
  def struct(acc: HashMap[String, Any], schema: Type, parent: NameValue = null): HashMap[String, Any] = {
    var map = acc

    for (nameValue <- values) {
      val value = nameValue.value
      val name = nameValue.name

      if (schema.isPrimitive) {
        map += name -> value
      } else {
        val parentType = schema.asGroupType()
        val subSchema = parentType.getType(name)
        if (subSchema.isPrimitive) {
          map += name -> value
        } else {
          parentType.getOriginalType match {
            case OriginalType.LIST =>
              val parentName = parent.name
              val record = value.asInstanceOf[SimpleRecord]
              map += parentName -> record.struct(map.getOrElse(parentName, List.empty[Any]).asInstanceOf[List[Any]])
            case OriginalType.MAP =>
              val record = value.asInstanceOf[SimpleRecord]
              val values = record.values
              val k = values(0).value

              val v = values(1).value match {
                case r: SimpleRecord =>
                  println(subSchema)
                  val nextSchema = subSchema.asGroupType().getType("value")
                  r.struct(HashMap.empty, nextSchema)
                case x => x
              }
              map += k.toString -> v
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
