import sbt._

/**
  * Dummy stdout logger for process running
  */
object StdOutLogger extends ProcessLogger {

  override def error(s: => String): Unit =
    ConsoleOut.systemOut.println(s)

  override def info(s: => String): Unit =
    ConsoleOut.systemOut.println(s)

  override def buffer[T](f: => T): T = f

}
