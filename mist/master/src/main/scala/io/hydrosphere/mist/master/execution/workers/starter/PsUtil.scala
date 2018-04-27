package io.hydrosphere.mist.master.execution.workers.starter

import scala.annotation.tailrec

trait PsUtil {

  /**
    * Correctly slit string into arguments for ProcessBuilder
    */
  def parseArguments(s: String): Seq[String] = {
    @tailrec
    def parse(in: String, curr: Vector[String]): Seq[String] = in.headOption match {
      case None => curr
      case Some(' ') => parse(in.dropWhile(_ == ' '), curr)
      case Some(sym) if sym == '"' || sym == ''' =>
        val (elem, tail) = in.tail.span(_ != sym)
        parse(tail.tail, curr :+ elem)
      case Some(_) =>
        val (elem, tail) = in.span(_ != ' ')
        parse(tail.tail, curr :+ elem)
    }
    parse(s, Vector.empty)
  }

}

object PsUtil extends PsUtil
