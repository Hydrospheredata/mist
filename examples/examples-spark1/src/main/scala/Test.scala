import mist.api.jdsl.JMistJob

object Test extends App {

  val cls = classOf[SimpleJavaContext]

  val v = cls.getSuperclass == classOf[Seq[_]]
  println(v)
//  cls.getSuperclass.getInterfaces.foreach(i => println(i.getName))
//  cls.getInterfaces.foreach(i => println(i.getName))
}
