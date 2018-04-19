package mist.api

sealed trait ArgInfo
case class InternalArgument(tags: Seq[String] = Seq.empty) extends ArgInfo
//TODO optional default value???
case class UserInputArgument(name: String, t: ArgType) extends ArgInfo

object ArgInfo {
  val StreamingContextTag = "streaming"
  val SqlContextTag = "sql"
  val HiveContextTag = "hive"
}

