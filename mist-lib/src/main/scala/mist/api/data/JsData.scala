package mist.api.data

import scala.collection.mutable
import scala.util._

/**
  * Json like data
  * We use our own data-structure to keep worker/master communications
  *  independent of third-party json libraries
  */
sealed trait JsData extends Serializable

case object JsUnit extends JsData {
  override def toString: String = "{}"
}

case object JsNull extends JsData {
  override def toString: String = "null"
}

final case class JsString(value: String) extends JsData {
  override def toString: String = value
}

object JsString {
  def of(s: String): JsString = JsString(s)
}

sealed trait JsBoolean extends JsData

case object JsTrue extends JsBoolean {
  override def toString: String = "true"
}
case object JsFalse extends JsBoolean {
  override def toString: String = "false"
}

object JsBoolean {
  def apply(b: Boolean): JsBoolean = b match {
    case true => JsTrue
    case false => JsFalse
  }

  def of(b: Boolean): JsBoolean = JsBoolean(b)
}

final case class JsNumber(v: BigDecimal) extends JsData {
  override def toString: String = v.toString()
}

object JsNumber {
  def apply(n: Short): JsNumber = new JsNumber(BigDecimal(n.toInt))
  def apply(n: Int): JsNumber = new JsNumber(BigDecimal(n))
  def apply(n: Long): JsNumber = new JsNumber(BigDecimal(n))
  def apply(n: Double): JsData = n match {
    case n if n.isNaN      => JsNull
    case n if n.isInfinity => JsNull
    case _                 => new JsNumber(BigDecimal(n))
  }
  def apply(n: BigInt): JsNumber = new JsNumber(BigDecimal(n))

  def of(n: Short): JsNumber = JsNumber(n)
  def of(n: Int): JsNumber = JsNumber(n)
  def of(n: Long): JsNumber = JsNumber(n)
  def of(n: Double): JsNumber = JsNumber(n)
  def of(n: BigInt): JsNumber = JsNumber(n)
}

final class JsMap(val map: Map[String, JsData]) extends JsData {

  override def toString: String = map.mkString("{", ",", "}")
  def fields: Seq[(String, JsData)] = map.toSeq
  def get(key: String): Option[JsData] = map.get(key)
  def fieldValue(key: String): JsData = get(key).getOrElse(JsNull)

  def addField(key: String, value: JsData): JsMap = JsMap(map + (key -> value))

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case JsMap(other) => other.equals(map)
      case _ => false
    }
  }

  override def hashCode(): Int = map.hashCode()

}

/**
  * java wrapper
  */
class Field(val key: String, val value: JsData)
object Field {
  def of(key: String, value: JsData): Field = new Field(key, value)
}


object JsMap {

  val empty: JsMap = JsMap()

  def apply(fields: (String, JsData)*): JsMap = {
    new JsMap(Map(fields: _*))
  }

  def apply(fields: Map[String, JsData]): JsMap = {
    // sometimes we can get map that can't be serialized
    // https://issues.scala-lang.org/browse/SI-7005
    val values = fields.toSeq
    new JsMap(Map(values: _*))
  }

  def unapply(arg: JsMap): Option[Map[String, JsData]] = Option(arg.map)

  def of(fields: java.util.List[Field]): JsMap = {
    import scala.collection.JavaConverters._
    val in = fields.asScala.map(f => f.key -> f.value)
    JsMap(in: _*)
  }
}

final case class JsList(list: Seq[JsData]) extends JsData {
  override def toString: String = list.mkString("[", ",", "]")
}

object JsList {
  def of(elems: java.util.List[JsData]): JsList = {
    import scala.collection.JavaConverters._
    JsList(elems.asScala)
  }
}

object JsData {
  import scala.collection.JavaConverters._

  def fromScala(a: Any): JsData = a match {
    case i: Int        => JsNumber(i)
    case d: Double     => JsNumber(d)
    case s: String     => JsString(s)
    case b: Boolean    => JsBoolean(b)
    case l: Seq[_]     => JsList(l.map(fromScala))
    case l: Array[_]   => JsList(l.map(fromScala))
    case m: Map[_, _] =>
      val norm = m.map({
        case (k: String, v) => k -> fromScala(v)
        case e => throw new IllegalArgumentException(s"Can not convert ${e._1} to MData(map keys should be instance of String)")
      })
      JsMap(norm)
    case opt: Option[_] if opt.isDefined => fromScala(opt.get)
    case _: Option[_] => JsNull
  }

  def untyped(d: JsMap): Map[String, Any] = {
    def convert(d: JsData): Any = d match {
      case m:JsMap => m.fields.map({case (k, v) => k -> convert(v)}).toMap
      case l:JsList => l.list.map(convert)
      case n:JsNumber => Try(n.v.toIntExact).orElse(Try(n.v.toIntExact)).getOrElse(n.v.toDouble)
      case JsString(s) => s
      case JsTrue => true
      case JsFalse => false
      case JsNull => null
      case JsUnit => Map.empty
    }
    d.fields.map({case (k, v) => k -> convert(v)}).toMap
  }

  def fromJava(a: Any): JsData = a match {
    case i: java.lang.Integer    => JsNumber(i)
    case d: java.lang.Double     => JsNumber(d)
    case s: java.lang.String     => JsString(s)
    case it: java.lang.Iterable[_] => JsList(it.asScala.map(fromJava).toSeq)
    case m: java.util.Map[_, _]  =>
      JsMap(
        m.entrySet().asScala
          .map(e=> e.getKey -> e.getValue)
          .collect {
            case (k: String, v) => k -> fromJava(v)
            case x => throw new IllegalArgumentException(s"Can not convert ${x._1} to MData(map keys should be instance of String)")
          }
          .toMap
      )
    case opt: java.util.Optional[_] if opt.isPresent => fromJava(opt.get())
    case _: java.util.Optional[_] => JsNull
  }

}
