package io.hydrosphere.mist.core.logging

case class Level(value: Int, name: String)

object Level {

  def fromInt(i: Int): Level = i match {
    case 1 => Debug
    case 2 => Info
    case 3 => Warn
    case 4 => Error
    case x => throw new IllegalArgumentException(s"Unknown level $i")
  }

  object Debug extends Level(1, "DEBUG")
  object Info extends Level(2, "INFO")
  object Warn extends Level(3, "WARN")
  object Error extends Level(4, "ERROR")
}

