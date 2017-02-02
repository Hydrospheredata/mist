package io.hydrosphere.mist.worker

private[mist] case class WorkerDescription (namespace: String, address: String) {
  override def toString: String = {
    namespace + "\t" + address
  }

  def length(): List[Int] = {
    List(namespace.length, address.length)
  }
}
