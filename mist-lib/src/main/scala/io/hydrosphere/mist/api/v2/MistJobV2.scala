package io.hydrosphere.mist.api.v2

import io.hydrosphere.mist.api.SetupConfiguration
import org.apache.spark.SparkContext
import shapeless.ops.adjoin.Adjoin
import shapeless.ops.hlist.{Prepend, IsHCons}
import shapeless.{Lazy, HNil, HList, ::}

trait JobP[+A] {
  type RunArgs <: HList
  def args: HList


  def run(map: Map[String, Any], sc: SparkContext): JobResult[A]

}

case class JobContext(
  setupConfiguration: SetupConfiguration,
  params: Map[String, Any]
)

trait JobDef[+A] {

  def jobFunc: JobContext => JobResult[A]

  def run(jobContext: JobContext): JobResult[A] = jobFunc(jobContext)
}

trait ArgExtractor[T] {
  type Out
  def extract(a: T, jobCtx: JobContext): ArgExtraction[Out]
}

object ArgExtractor {
  type Aux[T,  Out0] = ArgExtractor[T] {type Out = Out0 }
  def apply[A](implicit inst: ArgExtractor[A]): Aux[A, inst.Out] = inst
}

sealed trait ArgExtraction[+T]
case class Found[+T](value: T) extends ArgExtraction[T]
case class Missing[+T](description: String) extends ArgExtraction[T]

class JobBuilder[T <: HList, ArgOut <: HList, Tuple](args: T)(
  implicit
  extractor: ArgExtractor.Aux[T, ArgOut],
  tupler: shapeless.ops.hlist.Tupler.Aux[ArgOut, Tuple]
) {

  def toJobDef[R](f: Tuple => R): JobDef[R] = {
    new JobDef[R] {
      override def jobFunc: (JobContext) => JobResult[R] = {
        (ctx: JobContext) => {
          val extrResult = extractor.extract(args, ctx)
          extrResult match {
            case Found(realArg) =>
              val tuple = tupler(realArg)
              try {
                val r = f(tuple)
                JobSuccess(r)
              } catch {
                case e: Throwable => JobFailure(e)
              }
            case Missing(descr) => JobResult.failure(new IllegalArgumentException(s"Missing args: $descr"))
          }
        }
      }
    }
  }
}


trait MistJob extends JobInstances {

  def run: JobP[_]
}

object Main extends App {
  import shapeless.DepFn2

  trait Combiner[A, B] extends DepFn2[A, B] with Serializable

  trait LowPriorityCombiner {

    implicit def combinerX[H, T]: Combiner[H, T] = new Combiner[H, T] {
      override type Out = H :: T :: HNil

      override def apply(t: H, u: T): Out = t :: u :: HNil
    }
  }

  object Combiner extends LowPriorityCombiner {
    type Aux[A, B, Out0] = Combiner[A, B] {type Out = Out0}

    def apply[A, B](implicit comb: Combiner[A, B]): Combiner[A, B] = comb

    implicit def combinerHList[H <: HList, T]: Combiner[H, T] = new Combiner[H, T] {
      type Out = T :: H

      override def apply(t: H, u: T): T :: H = u :: t
    }
  }

  trait LowArgDef[A] { self =>

    def extract(ctx: JobContext): A

    def combine[B](other: LowArgDef[B])(implicit combiner: Combiner[B, A]): LowArgDef[combiner.Out] = {
      new LowArgDef[combiner.Out] {
        override def extract(ctx: JobContext): combiner.Out =
         combiner(other.extract(ctx), self.extract(ctx))
      }
    }


  }

  case class NamedArgDef[A](name: String) extends LowArgDef[A] {
    override def extract(ctx: JobContext): A = {
      println(s"call extract on $name")
      null.asInstanceOf[A]
    }
  }

  def printA[L <: HList](l: L)(implicit adjoin: Adjoin[L]): Unit = {
    println(adjoin(l))
  }
  val a = 1 :: (2 :: 4 :: HNil) :: HNil
  println(a)
  printA(a)

  val merged = NamedArgDef[String]("sad").combine(NamedArgDef[String]("z")).combine(NamedArgDef[String]("d"))
  merged.extract(new JobContext(null, null))

//
//  case class Argz[T](name: String)
//
//  trait miniExRes[+T]
//  case class miniS[+T](v: T) extends miniExRes[T]
//  case class miniF[+T](descr: String) extends miniExRes[T]
//
//  trait miniEx[T] {
//    def fromAny(a: Any): miniExRes[T]
//  }
//  def createMiniEx[T](f: Any => miniExRes[T]) = new miniEx[T] {
//    def fromAny(a: Any): miniExRes[T] = f(a)
//  }
//
//  implicit val intMiniEx = createMiniEx({
//    case i: Int => miniS(i)
//    case x => miniF(s"$x is not instance of Int")
//  })
//
//  implicit val strMiniEx = createMiniEx({
//    case s: String => miniS(s)
//    case x => miniF(s"$x is not instance of String")
//  })
//
//
//  implicit def extractorArgz[T](implicit mEx: miniEx[T]): ArgExtractor[Argz[T]] = {
//    new ArgExtractor[Argz[T]] {
//      type Out = T
//
//      override def extract(a: Argz[T], jobCtx: JobContext): ArgExtraction[T] = {
//        val data = jobCtx.params.get(a.name)
//        mEx.fromAny(data) match {
//          case miniS(value) => Found(value)
//          case miniF(ee) => Missing(ee)
//        }
//      }
//    }
//  }
//
//  implicit val hnilExtr: ArgExtractor[HNil] = new ArgExtractor[HNil] {
//    override type Out = HList
//
//    override def extract(a: HNil, jobCtx: JobContext): ArgExtraction[HList] = {
//      Found(HNil)
//    }
//  }
//
//  implicit def hlistExtr[H, T <: HList, OutH, OutT <: HList](
//    implicit
//    hE: ArgExtractor.Aux[H, OutH],
//    tE: ArgExtractor.Aux[T, OutT]
//  ): ArgExtractor[H :: T] = new ArgExtractor[H :: T] {
//      type Out = OutH :: OutT
//
//      override def extract(a: H :: T, jobCtx: JobContext): ArgExtraction[OutH :: OutT] = {
//        val fromA = hE.extract(a.head, jobCtx)
//        val fromB = tE.extract(a.tail, jobCtx)
//        (fromA, fromB) match {
//          case (Found(fa), Found(fb)) => Found(fa :: fb)
//          case (Missing(ma), Missing(mb)) => Missing(ma + " " + mb)
//          case (x @ Missing(ma), _) => Missing(ma)
//          case (_, x @ Missing(mb)) => Missing(mb)
//        }
//      }
//  }
//
//  import shapeless._
//  import shapeless.ops.hlist._
//  val a = Argz[Int]("nsadsad") :: HNil
//  a.mapCons()
//
//
//  val extr1 = ArgExtractor[Argz[Int]]
//  val extr = ArgExtractor[Argz[Int]::HNil]
  //new JobBuilder(Argz[Int]("n")::HNil)
}

