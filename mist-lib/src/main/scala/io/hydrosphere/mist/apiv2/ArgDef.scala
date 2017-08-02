package io.hydrosphere.mist.apiv2

import io.hydrosphere.mist.apiv2.internal.Combiner
import org.apache.spark.SparkContext

trait ArgExtraction[+A]
case class Extracted[+A](value: A) extends ArgExtraction[A]
case class Missing[+A](description: String) extends ArgExtraction[A]

trait ArgDef[A] { self =>

  def extract(ctx: JobContext): ArgExtraction[A]

  def combine[B](other: ArgDef[B])(implicit combiner: Combiner[B, A]): ArgDef[combiner.Out] = {
    new ArgDef[combiner.Out] {
      override def extract(ctx: JobContext): ArgExtraction[combiner.Out] = {
        val x = other.extract(ctx)
        val y = self.extract(ctx)
        (x, y) match {
          case (Extracted(b), Extracted(a)) => Extracted(combiner(b, a))
          case (Missing(err1), Missing(err2)) => Missing(err1 + "," + err2)
          case (x @ Missing(err1), _ ) => x.asInstanceOf[Missing[combiner.Out]]
          case (_, x @ Missing(err2)) => x.asInstanceOf[Missing[combiner.Out]]
        }
      }
    }
  }

  def &[B, Out](other: ArgDef[B])(implicit combiner: Combiner.Aux[B, A, Out]): ArgDef[Out] = {
    cmb0(other, combiner)
  }

  private def cmb0[B, Out](o: ArgDef[B], combiner: Combiner.Aux[B, A, Out]): ArgDef[Out] = {
    new ArgDef[Out] {
      override def extract(ctx: JobContext): ArgExtraction[Out] = {
        val x = o.extract(ctx)
        val y = self.extract(ctx)
        (x, y) match {
          case (Extracted(b), Extracted(a)) => Extracted(combiner(b, a))
          case (Missing(err1), Missing(err2)) => Missing(err1 + "," + err2)
          case (x @ Missing(err1), _ ) => x.asInstanceOf[Missing[Out]]
          case (_, x @ Missing(err2)) => x.asInstanceOf[Missing[Out]]
        }
      }
    }
  }

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

  def validated(f: A => Boolean, description: Option[String] = None): ArgDef[A] = {
    new ArgDef[A] {
      override def extract(ctx: JobContext): ArgExtraction[A] = {
        self.extract(ctx) match {
          case Extracted(a) if f(a) => Extracted(a)
          case Extracted(a) =>
            val message = s"Arg $self was rejected by validation rule" + description.map(s=>":"+ s).getOrElse("")
            Missing(message)
          case m @ Missing(err) => m
        }
      }
    }
  }

}

object ArgDef {

  def const[A](value: A): ArgDef[A] = new ArgDef[A] {
    override def extract(ctx: JobContext): ArgExtraction[A] = Extracted(value)
  }

  def missing[A](message: String): ArgDef[A] = new ArgDef[A] {
    override def extract(ctx: JobContext): ArgExtraction[A] = Missing(message)
  }

}

class NamedArgDef[A](name: String)(implicit fromAny: FromAny[A]) extends ArgDef[A] {
  override def extract(ctx: JobContext): ArgExtraction[A] = {
    val r = ctx.params.get(name).flatMap(a => fromAny(a))
    r match {
      case Some(a) => Extracted(a)
      case None => Missing(s"Argument $name is missing or has incorrect type")
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

