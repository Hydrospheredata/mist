package mist.api

sealed trait MData
case class MString(s: String) extends MData
case class MBoolean(b: Boolean) extends MData
case class MInt(i: Int) extends MData
case class MDouble(i: Double) extends MData
case class MMap(map: Map[String, MData]) extends MData
case class MList(list: Seq[MData]) extends MData
