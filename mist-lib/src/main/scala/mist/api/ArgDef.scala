package mist.api

import mist.api.args.{ArgCombiner, ToJobDef}

trait ArgExtraction[+A]
case class Extracted[+A](value: A) extends ArgExtraction[A]
case class Missing[+A](description: String) extends ArgExtraction[A]

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

  def apply[F, R](f: F)(implicit tjd: ToJobDef.Aux[A, F, R]): JobDef[R] = tjd(self, f)

}

object ArgDef extends FromAnyInstances {

  def create[A](f: JobContext => ArgExtraction[A]): ArgDef[A] = new ArgDef[A] {
      override def extract(ctx: JobContext): ArgExtraction[A] = f(ctx)
  }

  def const[A](value: A): ArgDef[A] = create(_ => Extracted(value))

  def missing[A](message: String): ArgDef[A] = create(_ => Missing(message))

}

