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

  private val scalaTypes: PartialFunction[Any, JsLikeData] = {
    case i: Int        => JsLikeNumber(i)
    case d: Double     => JsLikeNumber(d)
    case s: Short      => JsLikeNumber(s.toInt)
    case f: Float      => JsLikeNumber(f.toDouble)
    case l: Long       => JsLikeNumber(l)
    case b: BigInt     => JsLikeNumber(b)
    case b: BigDecimal => JsLikeNumber(b)
    case s: String     => JsLikeString(s)
    case b: Boolean    => JsLikeBoolean(b)
    case l: Seq[_]     => JsLikeList(l.map(fromAny))
    case l: Array[_]   => JsLikeList(l.map(fromAny))
    case m: Map[_, _] =>
      val norm = m.map({
        case (k: String, v) => k -> fromAny(v)
        case e => throw new IllegalArgumentException(s"Can not convert ${e._1} to MData(map keys should be instance of String)")
      })
      JsLikeMap(norm)
    case opt: Option[_] if opt.isDefined => fromAny(opt.get)
    case _: Option[_] => JsLikeNull
  }

  class SJIterable[T](private val jitb: java.lang.Iterable[T]) extends Iterable[T] {
    override def iterator: Iterator[T] = new SJIterator(jitb.iterator())
  }

  class SJIterator[T](private val jit: java.util.Iterator[T]) extends Iterator[T] {
    def hasNext: Boolean = jit hasNext

    def next: T = jit next
  }

  private val javaTypes: PartialFunction[Any, JsLikeData] = {
    case i: java.lang.Integer    => JsLikeNumber(i)
    case d: java.lang.Double     => JsLikeNumber(d)
    case s: java.lang.Short      => JsLikeNumber(s.toInt)
    case f: java.lang.Float      => JsLikeNumber(f.doubleValue())
    case l: java.lang.Long       => JsLikeNumber(l)
    case b: java.lang.Byte       => JsLikeNumber(b.intValue())
    case b: java.math.BigInteger => JsLikeNumber(b)
    case b: java.math.BigDecimal => JsLikeNumber(b)
    case s: java.lang.String     => JsLikeString(s)
    case it: java.lang.Iterable[_] => JsLikeList(new SJIterable(it).map(fromAny).toSeq)
    case m: java.util.Map[_, _]  =>
      JsLikeMap(
        m.keySet()
          .toArray()
          .collect {
            case s: String => s -> fromAny(m.get(s))
            case x => throw new IllegalArgumentException(s"Can not convert $x to MData(map keys should be instance of String)")
          }
          .toMap
      )
    case opt: java.util.Optional[_] if opt.isPresent => fromAny(opt.get())
    case _: java.util.Optional[_] => JsLikeNull
  }

  private val fromAnyType: PartialFunction[Any, JsLikeData] = scalaTypes orElse javaTypes orElse {
    case x if x == null => JsLikeNull
    case x => throw new IllegalArgumentException(s"Can convert $x to MData")
  }

  def fromAny(a: Any): JsLikeData = fromAnyType(a)

}
