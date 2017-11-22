package mist.api
import mist.api.args._

sealed trait ArgInfo
case class InternalArgument(tags: Seq[String] = Seq.empty) extends ArgInfo
case class UserInputArgument(name: String, t: ArgType) extends ArgInfo

object ArgInfo {
  val StreamingContextTag = "streaming"
  val SqlContextTag = "sql"
  val HiveContextTag = "hive"
}

trait ArgDescription[A] {
  def `type`: ArgType
  def apply(a: Any): Option[A]
}

trait ArgDescriptionInstances {

  def createInst[A](t: ArgType)(f: Any => Option[A]): ArgDescription[A] = new ArgDescription[A] {
    override def `type`: ArgType = t
    override def apply(a: Any): Option[A] = f(a)
  }

  implicit val forBoolean: ArgDescription[Boolean] = createInst(MBoolean) {
    case b: Boolean => Some(b)
    case _ => None
  }

  implicit val forInt: ArgDescription[Int] = createInst(MInt) {
    case i: Int => Some(i)
    case _ => None
  }


  implicit val forDouble: ArgDescription[Double] = createInst(MDouble) {
    case d: Double => Some(d)
    case _ => None
  }

  implicit val forString: ArgDescription[String] = createInst(MString) {
    case s: String => Some(s)
    case _ => None
  }

  implicit def forSeq[A](implicit u: ArgDescription[A]): ArgDescription[Seq[A]] =
    createInst(MList(u.`type`)) {
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

