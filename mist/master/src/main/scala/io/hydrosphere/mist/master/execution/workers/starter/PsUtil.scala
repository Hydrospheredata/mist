package io.hydrosphere.mist.master.execution.workers.starter

import scala.annotation.tailrec

trait PsUtil {

  /**
    * Correctly slit string into arguments for ProcessBuilder
    */
  def parseArguments(s: String): Seq[String] = {
    def safeTail(s: String): String = if (s.isEmpty) "" else s.tail
    @tailrec
    def parse(in: String, curr: Vector[String]): Seq[String] = in.headOption match {
      case None => curr
      case Some(' ') => parse(in.dropWhile(_ == ' '), curr)
      case Some(sym) if sym == '"' || sym == '\'' =>
        val (elem, rest) = in.tail.span(_ != sym)
        parse(safeTail(rest), curr :+ elem)
      case Some(_) =>
        val (elem, rest) = in.span(_ != ' ')
        parse(safeTail(rest), curr :+ elem)
    }
    parse(s, Vector.empty)
  }

}

object PsUtil extends PsUtil
