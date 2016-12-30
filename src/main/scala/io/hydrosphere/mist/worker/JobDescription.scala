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
  override def toString(): String = {
    uid + "\t" +  time + "\t" + namespace + "\t" + externalId.getOrElse(" " * 10) + "\t" + router.getOrElse(" " * 6)
  }
  def length(): List[Int] = {
    List(uid.length, time.length, namespace.length, externalId.getOrElse(" " * 10).length, router.getOrElse(" " * 6).length)
  }
}
