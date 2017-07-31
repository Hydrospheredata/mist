package io.hydrosphere.mist.api.v2

trait Extractor[T] {
  def fromAny(a: Any): Option[T]
}

object Extractor {

  implicit val strExt = new Extractor[String] {
    override def fromAny(a: Any): Option[String] = a  match {
        case s: String => Some(s)
        case _ => None
    }
  }

  implicit val intExt = new Extractor[Int] {
    override def fromAny(a: Any): Option[Int] = a  match {
      case i: Int => Some(i)
      case _ => None
    }
  }

  implicit def seqExt[T](implicit low: Extractor[T]): Extractor[Seq[T]] = new Extractor[Seq[T]] {
    override def fromAny(a: Any): Option[Seq[T]] = {
      a match {
        case s: Seq[_] =>
          val checked = s.map(s => low.fromAny(s))
          if (checked.exists(_.isEmpty)) None else Some(checked.collect({case Some(v) => v}))
        case x => None
      }
    }
  }

}
