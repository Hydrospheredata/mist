package io.hydrosphere.mist.api.v2

import io.hydrosphere.mist.api.v2.hlist._

import org.apache.spark.SparkContext

import Describer._
import Extractor._

import scala.annotation.tailrec

object JobAsd {

  type D[A] = Describer[A]
  type Sp = SparkContext

  trait JobP[+A] {
    type RunArgs <: HList
    def args: HList

    def run(args: RunArgs, ctx: Sp): JobResult[A]

    def extractValues(map: Map[String, Any]): Option[RunArgs] = {
      val inOptions = hackExtrc(args, map)//.asInstanceOf[Option[RunArgs]]
      fromOptions(inOptions)
    }

    def fromOptions(l: HList): Option[RunArgs] = {
      var result: HList = HNil

      var current = l
      while(current != HNil) {
        current match {
          case hlist.::(Some(v), t) =>
            result = hlist.::(v, result)
            current = t
          case hlist.::(None, t) =>
            return None
        }
      }
      Some(result.reverse.asInstanceOf[RunArgs])
    }

    def hackExtrc[T <: HList](xxxa: HList, map: Map[String, Any]): HList = {
      xxxa match {
        case hlist.::(h: Arg[_], t) =>
          hlist.::(h.extract(map), hackExtrc(t, map))
        case hlist.::(h, t) => throw new IllegalStateException("Invalid arguments")
        case HNil => HNil
      }
    }
  }

  case class JobMaker1[A: D](a1: Arg[A]) {
    def withContext[B](f: (A, Sp) => JobResult[B]): Job1[A, B] = Job1(a1, f)
  }

  case class Job1[A: D, B](a1: Arg[A], f: (A, Sp) => JobResult[B]) extends JobP[B] {
    type RunArgs = A :: HNil
    val args: Arg[A] :: HNil = a1 :: HNil

    def run(args: RunArgs, ctx: Sp): JobResult[B] = f(args.head, ctx)
  }

  case class JobMaker2[A: D, B: D](a1: Arg[A], a2: Arg[B]) {
    def withContext[X](f: (A, B, Sp) => JobResult[X]): Job2[A, B, X] = Job2(a1, a2, f)
  }
  case class Job2[A: D, B: D, X](a1: Arg[A], a2: Arg[B], f: (A, B, Sp) => JobResult[X]) extends JobP[X] {
    type RunArgs = A :: B :: HNil
    val args: Arg[A] :: Arg[B] :: HNil = ::(a1, a2 :: HNil)
    def run(args: RunArgs, ctx: Sp): JobResult[X] = {
      val a = args.head
      val b = args.tail.head
      f(a, b, ctx)
    }
  }

  def withArgs[A: D](a1: Arg[A]): JobMaker1[A] = JobMaker1(a1)
  def withArgs[A: D, B: D](a1: Arg[A], a2: Arg[B]): JobMaker2[A, B] = JobMaker2(a1, a2)
}


