package io.hydrosphere.mist.python



object PythonUtils {
  import scala.collection.JavaConverters._


  def toSeq[A](list: java.util.List[A]): Seq[A] = {
    list.asScala
  }

}
