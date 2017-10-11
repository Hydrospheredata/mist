package mist.api
import mist.api.args._

sealed trait ArgInfo
case object InternalArgument extends ArgInfo
case class UserInputArgument(name: String, t: ArgType) extends ArgInfo

trait ArgDescription[A] {
  def `type`: ArgType
  def apply(a: Any): Option[A]
}

//TODO: double, float, date?
trait ArgDescriptionInstances {

  def createInst[A](t: ArgType)(f: Any => Option[A]): ArgDescription[A] = new ArgDescription[A] {
    override def `type`: ArgType = t
    override def apply(a: Any): Option[A] = f(a)
  }

  implicit val forInt: ArgDescription[Int] = createInst(MTInt) {
    case i: Int => Some(i)
    case _ => None
  }

  implicit val forString: ArgDescription[String] = createInst(MTString) {
    case s: String => Some(s)
    case _ => None
  }

  implicit def forSeq[A](implicit u: ArgDescription[A]): ArgDescription[Seq[A]] =
    createInst(MTList(u.`type`)) {
      case seq: Seq[_] =>
        val optA = seq.map(a => u.apply(a))
        if (optA.exists(_.isEmpty)) {
          None
        } else {
          Some(optA.map(_.get))
        }
      case _ => None
    }

}

object ArgDescriptionInstances extends ArgDescriptionInstances

