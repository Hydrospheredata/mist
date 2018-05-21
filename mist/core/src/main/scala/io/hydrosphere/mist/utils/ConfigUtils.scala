package io.hydrosphere.mist.utils

import com.typesafe.config.Config

import scala.concurrent.duration.{Duration, FiniteDuration}

object ConfigUtils {

  implicit class ExtConfig(c: Config) {

    def getFiniteDuration(path: String): FiniteDuration =
      getScalaDuration(path) match {
        case f: FiniteDuration => f
        case _ => throw new IllegalArgumentException(s"Can not crate finite duration from $path")
      }

    def getScalaDuration(path: String): Duration = Duration(c.getString(path))

    def getOptString(path: String): Option[String] = getOpt(path, _.getString(path))
    def getOptInt(path: String): Option[Int] = getOpt(path, _.getInt(path))

    def getOpt[A](path: String, f: Config => A): Option[A] =
      if (c.hasPath(path)) Option(f(c)) else None
  }
}

