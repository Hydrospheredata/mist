package io.hydrosphere.mist.api.v2

import io.hydrosphere.mist.api.v2.Extractor.LowExtractor
import io.hydrosphere.mist.api.v2.hlist._
import org.apache.spark.SparkContext

case class Arg[T](val name: String)(implicit extrc: LowExtractor[T]) {

  def extract(map: Map[String, Any]): Option[T] = {
    map.get(name).flatMap(extrc.fromAny)
  }
}


trait Describer[A] {

  def describe(): String

}

object Describer {

  private def create[A](f: => String): Describer[A] =
    new Describer[A] {
      override def describe(): String = f
    }

  implicit val stringArg = create[String]("String argument")
  implicit val intArg = create[Int]("Int argument")
  implicit val sparkArg = create[SparkContext]("Spark Context!")

  implicit val hnilArg = create[HNil]("HNIL ???")

  implicit def seqArg[T](implicit t: Describer[T]): Describer[Seq[T]] = {
    create[Seq[T]]("Sequence:" + t.describe())
  }

  implicit def argDescriber[T](implicit t: Describer[T]): Describer[Arg[T]] = {
    create[Arg[T]]("ARG:" + t.describe())
  }

  implicit def hlistArg[H, T <: HList](
    implicit
     hA: Describer[H],
     tA: Describer[T]
  ): Describer[H :: T] = {
    create({
      hA.describe() + "\n" + tA.describe()
    })
  }

  def apply[A](a: A)(implicit d: Describer[A]) = d
}



