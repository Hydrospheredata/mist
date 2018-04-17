package mist.api.data

/**
  * Json like data
  * We use our own data-structure to keep worker/master communications
  *  independent of third-party json libraries
  */
sealed trait JsLikeData extends Serializable

case object JsLikeUnit extends JsLikeData {
  override def toString: String = "{}"
}

case object JsLikeNull extends JsLikeData {
  override def toString: String = "null"
}

final case class JsLikeString(s: String) extends JsLikeData {
  override def toString: String = s
}

final case class JsLikeBoolean(b: Boolean) extends JsLikeData {
  override def toString: String = b.toString
}

final case class JsLikeNumber(v: BigDecimal) extends JsLikeData {
  override def toString: String = v.toString()
}

object JsLikeNumber {
  def apply(n: Int): JsLikeNumber = new JsLikeNumber(BigDecimal(n))
  def apply(n: Long): JsLikeNumber = new JsLikeNumber(BigDecimal(n))
  def apply(n: Double): JsLikeData = n match {
    case n if n.isNaN      => JsLikeNull
    case n if n.isInfinity => JsLikeNull
    case _                 => new JsLikeNumber(BigDecimal(n))
  }
  def apply(n: BigInt): JsLikeNumber = new JsLikeNumber(BigDecimal(n))
}

final class JsLikeMap(val map: Map[String, JsLikeData]) extends JsLikeData {

  override def toString: String = map.mkString("{", ",", "}")
  def fields: Seq[(String, JsLikeData)] = map.toSeq
  def get(key: String): Option[JsLikeData] = map.get(key)
  def fieldValue(key: String): JsLikeData = get(key).getOrElse(JsLikeNull)

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case JsLikeMap(other) => other.equals(map)
      case any => false
    }
  }

  override def hashCode(): Int = map.hashCode()
}

object JsLikeMap {

  val empty: JsLikeMap = JsLikeMap()

  def apply(fields: (String, JsLikeData)*): JsLikeMap = {
    new JsLikeMap(Map(fields: _*))
  }

  def apply(fields: Map[String, JsLikeData]): JsLikeMap = {
    // sometimes we can get map that can't be serialized
    // https://issues.scala-lang.org/browse/SI-7005
    val values = fields.toSeq
    new JsLikeMap(Map(values: _*))
  }

  def unapply(arg: JsLikeMap): Option[Map[String, JsLikeData]] = Option(arg.map)

}

case class JsLikeList(list: Seq[JsLikeData]) extends JsLikeData {
  override def toString: String = list.mkString(",")
}

object JsLikeData {
  import scala.collection.JavaConverters._

  def fromScala(a: Any): JsLikeData = a match {
    case i: Int        => JsLikeNumber(i)
    case d: Double     => JsLikeNumber(d)
    case s: String     => JsLikeString(s)
    case b: Boolean    => JsLikeBoolean(b)
    case l: Seq[_]     => JsLikeList(l.map(fromScala))
    case l: Array[_]   => JsLikeList(l.map(fromScala))
    case m: Map[_, _] =>
      val norm = m.map({
        case (k: String, v) => k -> fromScala(v)
        case e => throw new IllegalArgumentException(s"Can not convert ${e._1} to MData(map keys should be instance of String)")
      })
      JsLikeMap(norm)
    case opt: Option[_] if opt.isDefined => fromScala(opt.get)
    case _: Option[_] => JsLikeNull
  }

  def fromJava(a: Any): JsLikeData = a match {
    case i: java.lang.Integer    => JsLikeNumber(i)
    case d: java.lang.Double     => JsLikeNumber(d)
    case s: java.lang.String     => JsLikeString(s)
    case it: java.lang.Iterable[_] => JsLikeList(it.asScala.map(fromJava).toSeq)
    case m: java.util.Map[_, _]  =>
      JsLikeMap(
        m.entrySet().asScala
          .map(e=> e.getKey -> e.getValue)
          .collect {
            case (k: String, v) => k -> fromJava(v)
            case x => throw new IllegalArgumentException(s"Can not convert ${x._1} to MData(map keys should be instance of String)")
          }
          .toMap
      )
    case opt: java.util.Optional[_] if opt.isPresent => fromJava(opt.get())
    case _: java.util.Optional[_] => JsLikeNull
  }

}
