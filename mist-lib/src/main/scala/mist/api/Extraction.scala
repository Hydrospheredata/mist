package mist.api

sealed trait Extraction[+A] { self =>

  def map[B](f: A => B): Extraction[B] = self match {
    case Extracted(a) => Extracted(f(a))
    case f:Failed => f.asInstanceOf[Extraction[B]]
  }

  def flatMap[B](f: A => Extraction[B]): Extraction[B] = self match {
    case Extracted(a) => f(a)
    case f:Failed => f.asInstanceOf[Extraction[B]]
  }

  def isFailed: Boolean = self match {
    case Extracted(a) => false
    case f:Failed => true
  }

  def isExtracted: Boolean = !isFailed

  def get(): A = self match {
    case Extracted(a) => a
    case f:Failed => throw new IllegalStateException(s"Extraction is failed: ${f}")
  }
}

final case class Extracted[+A](value: A) extends Extraction[A]
object Extracted {
  val unit: Extracted[Unit] = Extracted(())
}
sealed trait Failed extends Extraction[Nothing]

object Failed {

  final case class InternalError(msg: String) extends Failed
  final case class InvalidValue(msg: String) extends Failed
  final case class InvalidField(field: String, failure: Failed) extends Failed
  final case class InvalidType(expected: String, got: String) extends Failed
  final case class ComplexFailure(failures: Seq[Failed]) extends Failed
  final case class IncompleteObject(clazz: String, failure: Failed) extends Failed

  def invalidType(expected: String, got: String): InvalidType = InvalidType(expected, got)

  def toComplex(f1: Failed, f2: Failed): ComplexFailure = (f1, f2) match {
    case (ComplexFailure(in1), ComplexFailure(in2)) => ComplexFailure(in1 ++ in2)
    case (ComplexFailure(in), f:Failed) => ComplexFailure(in :+ f)
    case (f:Failed, ComplexFailure(in)) => ComplexFailure(f +: in)
    case (f1: Failed, f2: Failed) => ComplexFailure(Seq(f1, f2))
  }
}

object Extraction {

  def tryExtract[A](f: => A)(handleF: Throwable => Failed): Extraction[A] = {
    try {
      Extracted(f)
    } catch {
      case e: Throwable => handleF(e)
    }
  }

}
