package mist.api.data

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

case object JsTrue extends JsData {
  override def toString: String = "true"
}
case object JsFalse extends JsData {
  override def toString: String = "true"
}

object JsBoolean {
  def apply(b: Boolean): JsData = b match {
    case true => JsTrue
    case false => JsFalse
  }
}

final case class JsNumber(v: BigDecimal) extends JsData {
  override def toString: String = v.toString()
}

object JsNumber {
  def apply(n: Int): JsNumber = new JsNumber(BigDecimal(n))
  def apply(n: Long): JsNumber = new JsNumber(BigDecimal(n))
  def apply(n: Double): JsData = n match {
    case n if n.isNaN      => JsNull
    case n if n.isInfinity => JsNull
    case _                 => new JsNumber(BigDecimal(n))
  }
  def apply(n: BigInt): JsNumber = new JsNumber(BigDecimal(n))
}

final class JsMap(val map: Map[String, JsData]) extends JsData {

  override def toString: String = map.mkString("{", ",", "}")
  def fields: Seq[(String, JsData)] = map.toSeq
  def get(key: String): Option[JsData] = map.get(key)
  def fieldValue(key: String): JsData = get(key).getOrElse(JsNull)

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case JsMap(other) => other.equals(map)
      case any => false
    }
  }

  override def hashCode(): Int = map.hashCode()
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

}

final case class JsList(list: Seq[JsData]) extends JsData {
  override def toString: String = list.mkString(",")
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

  /** For running mist jobs directly from spark-submit **/
  def parse(s: String): Try[JsData] = {
    import org.json4s._

    def translateAst(in: JValue): JsData = in match {
      case JNothing => JsNull //???
      case JNull => JsNull
      case JString(s) => JsString(s)
      case JDouble(d) => JsNumber(d)
      case JDecimal(d) => JsNumber(d)
      case JInt(i) => JsNumber(i)
      case JBool(v) => JsBoolean(v)
      case JObject(fields) => JsMap(fields.map({case (k, v) => k -> translateAst(v)}): _*)
      case JArray(elems) => JsList(elems.map(translateAst))
    }

    Try(org.json4s.jackson.JsonMethods.parse(s, useBigDecimalForDouble = true)).map(json4sJs => translateAst(json4sJs))
  }

  def parseRoot(s: String): Try[JsMap] = parse(s).flatMap {
    case m:JsMap => Success(m)
    case _ => Failure(new IllegalArgumentException(s"Couldn't parse js object from input: $s"))
  }

  def formattedString(js: JsData): String = {
    import org.json4s._

    def translateAst(in: JsData): JValue = in match {
      case JsNull => JNull
      case JsString(s) => JString(s)
      case JsNumber(d) =>
        d.toBigIntExact() match {
          case Some(x) => JInt(x)
          case None => JDecimal(d)
        }
      case JsTrue => JBool(true)
      case JsFalse => JBool(false)
      case JsMap(fields) => JObject(fields.map({case (k, v) => k -> translateAst(v)}).toList)
      case JsList(elems) => JArray(elems.map(translateAst).toList)
    }
    org.json4s.jackson.JsonMethods.pretty(translateAst(js))
  }
}
