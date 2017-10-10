package mist.api

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext

trait JobDefInstances extends FromAnyInstances {

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



  def arg[A](name: String)(implicit a: FromAny[A]): ArgDef[A] = new NamedArgDef[A](name)

  def arg[A](name: String, default: A)(implicit a: FromAny[A]): ArgDef[A] = new NamedArgWithDefault[A](name, default)

  def optArg[A](name: String)(implicit a: FromAny[A]): ArgDef[Option[A]] = new OptionalNamedArgDef[A](name)

  val allArgs: ArgDef[Map[String, Any]] = new ArgDef[Map[String, Any]] {
    override def extract(ctx: JobContext): ArgExtraction[Map[String, Any]] = Extracted(ctx.params)
  }
}

object JobDefInstances extends JobDefInstances

trait JobDef[A] { self =>

  def invoke(ctx: JobContext): JobResult[A]
}

object JobDef {

  def instance[A](f: JobContext => JobResult[A]): JobDef[A] = new JobDef[A] {

    override def invoke(ctx: JobContext): JobResult[A] = f(ctx)

  }
}
