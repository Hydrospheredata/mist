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

case class JsLikeString(s: String) extends JsLikeData {
  override def toString: String = s
}

case class JsLikeBoolean(b: Boolean) extends JsLikeData {
  override def toString: String = b.toString
}

case class JsLikeInt(i: Int) extends JsLikeData {
  override def toString: String = i.toString
}

case class JsLikeDouble(i: Double) extends JsLikeData {
  override def toString: String = i.toString
}

case class JsLikeMap(map: Map[String, JsLikeData]) extends JsLikeData {
  override def toString: String = map.toString
}

case class JsLikeList(list: Seq[JsLikeData]) extends JsLikeData {
  override def toString: String = list.toString
}

case class JsLikeOption(opt: Option[JsLikeData]) extends JsLikeData {
  override def toString: String = opt.toString
}

object JsLikeData {

  def fromAny(a: Any): JsLikeData = a match {
    case i: Int => JsLikeInt(i)
    case s: String => JsLikeString(s)
    case d: Double => JsLikeDouble(d)
    case b: Boolean => JsLikeBoolean(b)
    case l: Seq[_] => JsLikeList(l.map(fromAny))
    case l: Array[_] => JsLikeList(l.map(fromAny))
    case m: Map[_, _] =>
      val norm = m.map({
        case (k: String, v) => k -> fromAny(v)
        case _ => throw new IllegalArgumentException(s"Can not convert $a to MData(map keys should be instance of String)")
      })
      JsLikeMap(norm)
    case opt: Option[_] =>
      val v = opt.map(fromAny)
      JsLikeOption(v)

    case x => throw new IllegalArgumentException(s"Can convert $x to MData")
  }
}
