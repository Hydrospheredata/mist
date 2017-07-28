package io.hydrosphere.mist.api.v2

import io.hydrosphere.mist.api.v2.hlist._

trait JobP[+A] {
  type RunArgs <: HList
  def args: HList

  def extractValues(map: Map[String, Any]): Option[RunArgs] = {
    val inOptions = hackExtrc(args, map)
    fromOptions(inOptions)
  }

  private def fromOptions(l: HList): Option[RunArgs] = {
    var result: HList = HNil

    var current = l
    while(current != HNil) {
      current match {
        case Some(v) :: t =>
          result = ::(v,result)
          current = t
        case ::(None, t) =>
          return None
        case ::(x, t) => throw new IllegalStateException("Illegal state")
        case HNil => throw new IllegalStateException("Illegal state")
      }
    }
    Some(result.reverse.asInstanceOf[RunArgs])
  }

  private def hackExtrc[T <: HList](xxxa: HList, map: Map[String, Any]): HList = {
    xxxa match {
      case ::(h: Arg[_], t) =>
        ::(h.extract(map), hackExtrc(t, map))
      case ::(h, t) => throw new IllegalStateException("Invalid arguments")
      case HNil => HNil
    }
  }
}

trait MistJob extends Jobs {

  def run: JobP[_]
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


