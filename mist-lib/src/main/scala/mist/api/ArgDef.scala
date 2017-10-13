package mist.api

import mist.api.args.{ArgCombiner, ToJobDef}

import scala.reflect.ClassTag

trait ArgExtraction[+A]
case class Extracted[+A](value: A) extends ArgExtraction[A]
case class Missing[+A](description: String) extends ArgExtraction[A]

trait ArgDef[A] { self =>

  def describe(): Seq[ArgInfo]

  def extract(ctx: JobContext): ArgExtraction[A]

  def combine[B](other: ArgDef[B])
      (implicit cmb: ArgCombiner[A, B]): ArgDef[cmb.Out] = cmb(self, other)

  def &[B](other: ArgDef[B])
    (implicit cmb: ArgCombiner[A, B]): ArgDef[cmb.Out] = cmb(self, other)

  def map[B](f: A => B): ArgDef[B] = {
    new ArgDef[B] {

      override def describe(): Seq[ArgInfo] = self.describe()

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
      override def describe(): Seq[ArgInfo] = self.describe()
      override def extract(ctx: JobContext): ArgExtraction[B] =
        self.extract(ctx) match {
          case Extracted(a) => f(a)
          case m @ Missing(err) => m.asInstanceOf[Missing[B]]
        }
    }
  }

  def validated(f: A => Boolean, reason: String = ""): ArgDef[A] = {
    self.flatMap(a => {
      if (f(a)) Extracted(a)
      else {
        val descr = if (reason.isEmpty) "" else " :" + reason
        val message = s"Arg was rejected by validation rule" + descr
        Missing(message)
      }
    })
  }

  def apply[F, R](f: F)(implicit tjd: ToJobDef.Aux[A, F, R]): JobDef[R] = tjd(self, f)

}

object ArgDef {

  def create[A](info: ArgInfo)(f: JobContext => ArgExtraction[A]): ArgDef[A] = new ArgDef[A] {
      def describe(): Seq[ArgInfo] = Seq(info)
      def extract(ctx: JobContext): ArgExtraction[A] = f(ctx)
  }

  def const[A](value: A): ArgDef[A] = create(InternalArgument)(_ => Extracted(value))

  def missing[A](message: String): ArgDef[A] = create(InternalArgument)(_ => Missing(message))

}

