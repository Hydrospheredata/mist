package io.hydrosphere.mist.api.v2

import io.hydrosphere.mist.api.SetupConfiguration
import org.apache.spark.SparkContext
import shapeless.{HList, ::, HNil}
import shapeless.ops.hlist.Reverse._

trait JobP[+A] {
  type RunArgs <: HList
  def args: HList


  def run(map: Map[String, Any], sc: SparkContext): JobResult[A]

}

trait MistJob extends JobInstances {

  def run: JobP[_]
}

object Testf extends App {
  import shapeless._
  import shapeless.ops.hlist._
  import shapeless.poly._

  val xx =  Arg[Int]("n") :: Arg[String]("name") :: HNil

  val map = Map(
    "n" -> 4,
    "name" -> "namename"
  )

  object choose extends (Arg ~> Option) {
    override def apply[T](f: Arg[T]): Option[T] = f.extract(map)
  }

  val list = xx.toList

  val z = list.map(a => a.extract(map))
  println(z)

  z.exists({case opt:Option[_] => opt.isEmpty})


}

object MyMistJob extends MistJob {

  import Extractor._

  override def run =
    withArgs(
      Arg[Int]("n"),
      Arg[String]("name")
    ).withContext((n, name, sparkContext) => {
      JobSuccess(1)
    })
}


