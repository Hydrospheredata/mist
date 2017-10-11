package mist.api.jdsl

import mist.api._
import mist.api.ArgDef._
import java.lang.{Integer => JavaInt}

import org.apache.spark.api.java.JavaSparkContext
import org.json4s.JValue
import FuncOps._
import BaseContexts._
import mist.api.args.{ArgType, MTInt}
import mist.api.data._

//case class Args1[T1](a1: ArgDef[T1]) {
//
//  def onJSparkContext[R](f: JScFunc2[T1, RetVal[R]]): JobDef[R] = {
//
//    val func = f.map(_.encoded())
//    (a1 & javaSparkContext).apply(func)
//  }
//}
//
//case class Args2[T1, T2](a1: ArgDef[T1], a2: ArgDef[T2]) {
//
//  def onJSparkContext[R](f: JScFunc3[T1, T2, RetVal[R]]): JobDef[R] = {
//    val func = f.map(_.encoded())
//    (a1 & a2 & javaSparkContext).apply(func)
//  }
//}

case class RetVal[T](value: T, encoder: Encoder[T]) {
  def encoded(): Any = encoder(value)
}

trait RetVals {

  def intRetVal(i: JavaInt): RetVal[JavaInt] = RetVal(i, new Encoder[JavaInt] {
    override def apply(a: JavaInt): MData = MInt(a)
  })

  def stringRetVal(s: String): RetVal[String] = RetVal(s, DefaultEncoders.StringEncoder)


}

object RetVals extends RetVals

trait JArgsDef extends ArgDescriptionInstances {

  import JobDefInstances._

  implicit val jInt = new ArgDescription[JavaInt] {
    override def `type`: ArgType = MTInt
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

//abstract class JMistJob[T] extends MistJob[T] with JArgsDef with RetVals
