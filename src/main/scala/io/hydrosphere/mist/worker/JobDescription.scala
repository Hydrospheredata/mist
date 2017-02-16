package io.hydrosphere.mist.worker

private[mist] class JobDescription (val uid: () => String,
                                    val time: String,
                                    val namespace: String,
                                    val externalId: Option[String] = None,
                                    val router: Option[String] = None)

private[mist] class JobDescriptionSerializable (val uid: String,
                                                val time: String,
                                                val namespace: String,
                                                val externalId: Option[String] = None,
                                                val router: Option[String] = None)
  extends Serializable {
  val separator = 5
  override def toString(): String = {
    uid + " " * separator + "\t" +
      time + " " * separator + "\t" +
      namespace + " " * separator + "\t" +
      externalId.getOrElse(" " * 10) + " " * separator +
      "\t" + router.getOrElse(" " * 6)
  }
  def length(): List[Int] = {
    List(uid.length + separator,
      time.length + separator,
      namespace.length + separator,
      externalId.getOrElse(" " * 10).length + separator,
      router.getOrElse(" " * 6).length)
  }
}
