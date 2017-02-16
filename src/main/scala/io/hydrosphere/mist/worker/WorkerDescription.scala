package io.hydrosphere.mist.worker

private[mist] case class WorkerDescription (uid: String, namespace: String, address: String, blackSpot: Boolean = false) {

  val separator = 5

  override def toString: String = {
    namespace + " " * separator + "\t" +
      address + " " * separator + "\t" +
      uid + " " * separator + "\t" +
      blackSpot.toString
  }

  def length(): List[Int] = {
    List(namespace.length + separator,
      address.length + separator,
      uid.length + separator,
      blackSpot.toString.length)
  }
}
