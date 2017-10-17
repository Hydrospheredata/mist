package mist.api.data

/**
  * Json like data
  * We use our own data-structure to keep worker/master communications
  *  independent of third-party json libraries
  */
sealed trait JsLikeData

case object JsLikeUnit extends JsLikeData {
  override def toString: String = "{}"
}

case object JsLikeNull extends JsLikeData {
  override def toString: String = "null"
}

case class JsLikeString(s: String) extends JsLikeData {
  override def toString: String = s
}

case class JsLikeBoolean(b: Boolean) extends JsLikeData {
  override def toString: String = b.toString
}

case class JsLikeNumber(v: BigDecimal) extends JsLikeData {
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

case class JsLikeMap(map: Map[String, JsLikeData]) extends JsLikeData {
  override def toString: String = map.mkString("{", ",", "}")
  def fields: Seq[(String, JsLikeData)] = map.toSeq
  def get(key: String): Option[JsLikeData] = map.get(key)
}

object JsLikeMap {

  def apply(fields: (String, JsLikeData)*): JsLikeMap = JsLikeMap(fields.toMap)

}

case class JsLikeList(list: Seq[JsLikeData]) extends JsLikeData {
  override def toString: String = list.mkString(",")
}


object JsLikeData {

  def fromAny(a: Any): JsLikeData = a match {
    case i: Int     => JsLikeNumber(i)
    case d: Double  => JsLikeNumber(d)
    case s: Short   => JsLikeNumber(s.toInt)
    case f: Float   => JsLikeNumber(f.toDouble)
    case l: Long    => JsLikeNumber(l)
    case s: String  => JsLikeString(s)
    case b: Boolean => JsLikeBoolean(b)
    case l: Seq[_]  => JsLikeList(l.map(fromAny))
    case l: Array[_] => JsLikeList(l.map(fromAny))
    case m: Map[_, _] =>
      val norm = m.map({
        case (k: String, v) => k -> fromAny(v)
        case _ => throw new IllegalArgumentException(s"Can not convert $a to MData(map keys should be instance of String)")
      })
      JsLikeMap(norm)
    case opt: Option[_] if opt.isDefined => fromAny(opt.get)
    case opt: Option[_] => JsLikeNull

    case x if x == null => JsLikeNull
    case x => throw new IllegalArgumentException(s"Can convert $x to MData")
  }
}
