package mist.api

case class RuntimeJobInfo(
  id: String,
  workerId: String
)

object RuntimeJobInfo {
  val Unknown = RuntimeJobInfo("unknown", "unknown")
}
