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

}
