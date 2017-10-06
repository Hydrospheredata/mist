package mist.api.jdsl

import org.apache.spark.api.java.JavaSparkContext

@SerialVersionUID(1L)
trait Func1[-T1, +R] extends java.io.Serializable {
  @throws(classOf[Exception])
  def apply(a1: T1): R
}

@SerialVersionUID(1L)
trait Func2[-T1, -T2, +R] extends java.io.Serializable {
  @throws(classOf[Exception])
  def apply(a1: T1, a2: T2): R
}

@SerialVersionUID(1L)
trait Func3[-T1, -T2, -T3, +R] extends java.io.Serializable {
  @throws(classOf[Exception])
  def apply(a1: T1, a2: T2, a3: T3): R
}

trait JScFunc1[R] extends Func1[JavaSparkContext, R] {
  override def apply(sc: JavaSparkContext): R
}

trait JScFunc2[T1, R] extends Func2[T1, JavaSparkContext, R] {
  override def apply(a1: T1, sc: JavaSparkContext): R
}

trait JScFunc3[T1, T2, R] extends Func3[T1, T2, JavaSparkContext, R] {
  override def apply(a1: T1, a2: T2, sc: JavaSparkContext): R
}

