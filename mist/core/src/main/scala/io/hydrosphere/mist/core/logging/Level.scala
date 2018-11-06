package io.hydrosphere.mist.core.logging

sealed trait Level {
  def value: Int
  def name: String
}

object Level {

  def fromInt(i: Int): Level = i match {
    case 1 => Debug
    case 2 => Info
    case 3 => Warn
    case 4 => Error
    case x => throw new IllegalArgumentException(s"Unknown level $i")
  }

  object Debug extends Level { val value = 1; val name = "DEBUG" }
  object Info  extends Level { val value = 2; val name = "INFO"  }
  object Warn  extends Level { val value = 3; val name = "WARN"  }
  object Error extends Level { val value = 4; val name = "ERROR" }
}

