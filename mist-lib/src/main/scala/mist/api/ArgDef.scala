package mist.api

import mist.api.jdsl.RetVal
import org.apache.spark.SparkContext
import org.json4s.JsonAST.JValue
import shapeless.ops.adjoin.Adjoin
import shapeless.ops.function.FnToProduct
import shapeless.{::, HList, HNil, Lazy}

import scala.annotation.implicitNotFound

trait ArgExtraction[+A]
case class Extracted[+A](value: A) extends ArgExtraction[A]
case class Missing[+A](description: String) extends ArgExtraction[A]

trait ArgCombiner[A, B] extends Serializable {
  type Out
  def apply(a: ArgDef[A], b: ArgDef[B]): ArgDef[Out]
}

object ArgCombiner {

  type Aux[A, B, Out0] = ArgCombiner[A, B] { type Out = Out0 }

  implicit def combiner[A, B](implicit adj: Adjoin[A :: B :: HNil]): Aux[A, B, adj.Out] = new ArgCombiner[A, B] {
    type Out = adj.Out

    override def apply(a: ArgDef[A], b: ArgDef[B]): ArgDef[Out] = {
      new ArgDef[adj.Out] {
        override def extract(ctx: JobContext): ArgExtraction[adj.Out] = {
          (b.extract(ctx), a.extract(ctx)) match {
            case (Extracted(be), Extracted(ae)) =>
              val out = adj(ae :: be :: HNil)
              Extracted(out)
            case (Missing(errB), Missing(errA)) => Missing(errA + ", " + errB)
            case (x @ Missing(err1), _ ) => x.asInstanceOf[Missing[adj.Out]]
            case (_, x @ Missing(err2)) => x.asInstanceOf[Missing[adj.Out]]
          }
        }
      }

    }
  }
}

trait ArgDef[A] { self =>

  def extract(ctx: JobContext): ArgExtraction[A]

  def combine[B](other: ArgDef[B])
      (implicit cmb: ArgCombiner[A, B]): ArgDef[cmb.Out] = cmb(self, other)

  def &[B](other: ArgDef[B])
    (implicit cmb: ArgCombiner[A, B]): ArgDef[cmb.Out] = cmb(self, other)

  def map[B](f: A => B): ArgDef[B] = {
    new ArgDef[B] {
      override def extract(ctx: JobContext): ArgExtraction[B] = {
        self.extract(ctx) match {
          case Extracted(a) => Extracted(f(a))
          case m @ Missing(err) => m.asInstanceOf[Missing[B]]
        }
      }
    }
  }

  def flatMap[B](f: A => ArgExtraction[B]): ArgDef[B] = {
    new ArgDef[B] {
      override def extract(ctx: JobContext): ArgExtraction[B] =
        self.extract(ctx) match {
          case Extracted(a) => f(a)
          case m @ Missing(err) => m.asInstanceOf[Missing[B]]
        }
    }
  }

  def validated(f: A => Boolean, description: Option[String] = None): ArgDef[A] = {
    self.flatMap(a => {
      if (f(a)) Extracted(a)
      else {
        val message = s"Arg $self was rejected by validation rule" + description.map(s => ":" + s).getOrElse("")
        Missing(message)
      }
    })
  }

  @implicitNotFound(msg = "Encoder ${R}")
  def apply[F, R, H <: HList](f: F)
      (implicit
         enc: Encoder[R],
         norm: Normalizer.Aux[A, H],
         fntp: FnToProduct.Aux[F, H => R]): JobDef[R] = {

    new JobDef[R] {

      override def invoke(ctx: JobContext): JobResult[Any]= {
        self.extract(ctx) match {
          case Extracted(args) =>
            try {
              val result = fntp(f)(norm(args))
              JobResult.success(enc(result))
            } catch {
              case e: Throwable => JobResult.failure(e)
            }
          case Missing(msg) =>
            val e = new IllegalArgumentException(s"Arguments does not conform to job [$msg]")
            JobResult.failure(e)
        }
      }

    }
  }

  def apply2[F, R, H <: HList](f: F)
      (implicit
         norm: Normalizer.Aux[A, H],
         fntp: FnToProduct.Aux[F, H => Any]): JobDef[R] = {

    new JobDef[R] {

      override def invoke(ctx: JobContext): JobResult[Any]= {
        self.extract(ctx) match {
          case Extracted(args) =>
            try {
              val result = fntp(f)(norm(args))
              JobResult.success(result)
            } catch {
              case e: Throwable => JobResult.failure(e)
            }
          case Missing(msg) =>
            val e = new IllegalArgumentException(s"Arguments does not conform to job [$msg]")
            JobResult.failure(e)
        }
      }

    }
  }

}

object ArgDef extends FromAnyInstances {

  def const[A](value: A): ArgDef[A] = new ArgDef[A] {
    override def extract(ctx: JobContext): ArgExtraction[A] = Extracted(value)
  }

  def missing[A](message: String): ArgDef[A] = new ArgDef[A] {
    override def extract(ctx: JobContext): ArgExtraction[A] = Missing(message)
  }

  class NamedArgDef[A](name: String)(implicit fromAny: FromAny[A]) extends ArgDef[A] {
    override def extract(ctx: JobContext): ArgExtraction[A] = {
      ctx.params.get(name).flatMap(a => fromAny(a)) match {
        case Some(a) => Extracted(a)
        case None => Missing(s"Argument $name is missing or has incorrect type")
      }
    }
  }

  class NamedArgWithDefault[A](name: String, default: A)(implicit fromAny: FromAny[A]) extends ArgDef[A] {
    override def extract(ctx: JobContext): ArgExtraction[A] = {
      ctx.params.get(name) match {
        case Some(any) => fromAny(any) match {
          case Some(v) => Extracted(v)
          case None => Missing(s"Argument $name is missing or has incorrect type")
        }
        case None => Extracted(default)
      }
    }

  }

  class OptionalNamedArgDef[A](name: String)(implicit fromAny: FromAny[A]) extends ArgDef[Option[A]] {
    override def extract(ctx: JobContext): ArgExtraction[Option[A]] = {
      ctx.params.get(name) match {
        case Some(any) =>
          fromAny(any) match {
            case Some(a) => Extracted(Some(a))
            case None => Missing(s"Argument $name has incorrect type")
          }
        case None => Extracted(None)
      }
    }
  }

  class SparkContextArgDef extends ArgDef[SparkContext] {
    override def extract(ctx: JobContext): ArgExtraction[SparkContext] =
      Extracted(ctx.setupConfiguration.context)
  }

  def arg[A](name: String)(implicit a: FromAny[A]): ArgDef[A] = new NamedArgDef[A](name)

  def arg[A](name: String, default: A)(implicit a: FromAny[A]): ArgDef[A] = new NamedArgWithDefault[A](name, default)

  def optArg[A](name: String)(implicit a: FromAny[A]): ArgDef[Option[A]] = new OptionalNamedArgDef[A](name)

  val allArgs: ArgDef[Map[String, Any]] = new ArgDef[Map[String, Any]] {
    override def extract(ctx: JobContext): ArgExtraction[Map[String, Any]] = Extracted(ctx.params)
  }
}

