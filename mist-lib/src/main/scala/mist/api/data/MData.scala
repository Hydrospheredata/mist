package mist.api.data

sealed trait MData
case object MUnit extends MData {
  override def toString: String = "{}"
}

case class MString(s: String) extends MData {
  override def toString: String = s
}

case class MBoolean(b: Boolean) extends MData {
  override def toString: String = b.toString
}

case class MInt(i: Int) extends MData {
  override def toString: String = i.toString
}

case class MDouble(i: Double) extends MData {
  override def toString: String = i.toString
}

case class MMap(map: Map[String, MData]) extends MData {
  override def toString: String = map.toString
}

case class MList(list: Seq[MData]) extends MData {
  override def toString: String = list.toString()
}

case class MOption(opt: Option[MData]) extends MData {
  override def toString: String = opt.toString()
}

object MData {

  def fromAny(a: Any): MData = a match {
    case i: Int => MInt(i)
    case s: String => MString(s)
    case d: Double => MDouble(d)
    case b: Boolean => MBoolean(b)
    case l: Seq[_] => MList(l.map(fromAny))
    case m: Map[_, _] =>
      val norm = m.map({
        case (k: String, v) => k -> fromAny(a)
        case _ => throw new IllegalArgumentException(s"Can not convert $a to MData(map keys should be instance of String)")
      })
      MMap(norm)
    case opt: Option[_] =>
      val v = opt.map(fromAny)
      MOption(v)

    case x => throw new IllegalArgumentException(s"Can convert $x to MData")
  }
}
