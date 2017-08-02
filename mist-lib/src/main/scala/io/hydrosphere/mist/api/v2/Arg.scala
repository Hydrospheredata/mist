package io.hydrosphere.mist.api.v2

case class Arg[T](val name: String)(implicit extrc: Extractor[T]) {

  def extract(map: Map[String, Any]): Option[T] = {
    map.get(name).flatMap(extrc.fromAny)
  }

  def seq: Arg[Seq[T]] = new Arg[Seq[T]](name)

  def map[B](f: T=> B): Arg[B] = {
    implicit val extr2 = new Extractor[B] {
      override def fromAny(a: Any): Option[B] =
        extrc.fromAny(a).map(f)
    }
    new Arg[B](name)
  }
}

object Arg {

  def intArg(name: String): Arg[Int] = Arg(name)
  def jIntArg(name: String): Arg[java.lang.Integer] = intArg(name).map(java.lang.Integer.valueOf)
  def strArg(name: String): Arg[String] = Arg[String](name)
}

//sealed trait ArgRes[+T]
//case object Missing extends ArgRes[Nothing]
//case class Value[+T](value: T) extends ArgRes[T]
//
//trait Arg2[T] {
//
//  def extract(map: Map[String, Any]): Option[T]
//
//}
//
//class BaseArg[T] extends Arg2[T](implicit) {
//  override def extract(map: Map[String, Any]): Option[T] = ???
//}
