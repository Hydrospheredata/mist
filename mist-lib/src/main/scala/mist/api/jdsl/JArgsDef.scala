package mist.api.jdsl

import mist.api._
import mist.api.ArgDef._
import java.lang.{Integer => JavaInt}

import org.apache.spark.api.java.JavaSparkContext
//import org.json4s.JValue
//import FuncOps._
import BaseContexts._
import mist.api.args.{ArgType, MInt}
import mist.api.data._

//case class Args1[T1](a1: ArgDef[T1]) {
//
//  def onSparkContext[R](f: Func2[T1, JavaSparkContext, RetVal[R]]): JJobDef[R] = {
//    val job = (a1 & javaSparkContext).apply(f.toScalaFunc)
//    new JJobDef(job)
//  }
//}
//
//case class Args2[T1, T2](a1: ArgDef[T1], a2: ArgDef[T2]) {
//
//  def onSparkContext[R](f: Func3[T1, T2, JavaSparkContext, RetVal[R]]): JJobDef[R] = {
//    val job = (a1 & a2 & javaSparkContext).apply(f.toScalaFunc)
//    new JJobDef(job)
//  }
//}

case class RetVal[T](value: T, encoder: Encoder[T]) {
  def encoded(): JsLikeData = encoder(value)
}

trait RetVals {

  def intRetVal(i: JavaInt): RetVal[JavaInt] = RetVal(i, new Encoder[JavaInt] {
    override def apply(a: JavaInt): JsLikeData = JsLikeInt(a)
  })

  def stringRetVal(s: String): RetVal[String] = RetVal(s, DefaultEncoders.StringEncoder)

}

object RetVals extends RetVals

trait JArgsDef extends ArgDescriptionInstances {

  import JobDefInstances._

  implicit val jInt = new ArgDescription[JavaInt] {
    override def `type`: ArgType = MInt
    override def apply(a: Any): Option[JavaInt] = a match {
      case i: Int => Some(new JavaInt(i))
      case _ => None
    }
}

  def intArg(name: String): ArgDef[Integer] = arg[Integer](name)
  def stringArg(name: String): ArgDef[String] = arg[String](name)

//  def withArgs[T1](a1: ArgDef[T1]): Args1[T1] = Args1(a1)
//  def withArgs[T1, T2](a1: ArgDef[T1], a2: ArgDef[T2]): Args2[T1, T2] = Args2(a1, a2)
}

class JJobDef[T](val jobDef: JobDef[RetVal[T]])

abstract class JMistJob[T] extends JArgsDef with JJobDefinition {

  def defineJob: JJobDef[T]

  final def execute(ctx: JobContext): JobResult[JsLikeData] = {
    defineJob.jobDef.invoke(ctx) match {
      case JobSuccess(v) => JobSuccess(v.encoded())
      case f: JobFailure[_] => f.asInstanceOf[JobFailure[JsLikeData]]
    }
  }
}

