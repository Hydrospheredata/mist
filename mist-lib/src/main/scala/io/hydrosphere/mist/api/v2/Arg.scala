package io.hydrosphere.mist.api.v2

case class Arg[T](val name: String)(implicit extrc: Extractor[T]) {

  def extract(map: Map[String, Any]): Option[T] = {
    map.get(name).flatMap(extrc.fromAny)
  }

  def seq: Arg[Seq[T]] = new Arg[Seq[T]](name)
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
