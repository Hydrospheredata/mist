package io.hydrosphere.mist.worker

private[mist] case class WorkerDescription (val namespace: String,
                                            val address: String) {
  override def toString(): String = {
    namespace + "\t" + address
  }

  def length(): List[Int] = {
    List(namespace.length, address.length)
  }
}
