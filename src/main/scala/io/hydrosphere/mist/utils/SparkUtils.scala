package io.hydrosphere.mist.utils

/*
  Utils for Spark.
 */
object SparkUtils {

  /*
    Utils for determining Spark capabilities and versions.
   */
  object Version {
    private val versionRegex = "(\\d+)\\.(\\d+).*".r

    def sparkVersion: String = util.Properties.propOrNone("sparkVersion").getOrElse("[1.5.2, )")

    val areSessionsSupported: Boolean = {
      sparkVersion match {
        case versionRegex(major, minor) if major.toInt > 1 => true
        case _ => false
      }
    }

    val isOldVersion = {
      sparkVersion match {
        case versionRegex(major, minor) if major.toInt == 1 && List(4, 5, 6).contains(minor.toInt) => true
        case versionRegex(major, minor) if major.toInt > 1 => false
        case _ => true
      }
    }

    def is2_1: Boolean = {
      sparkVersion match {
        case versionRegex(major, minor) if major.toInt == 2 && minor.toInt == 1 => true
        case _ => false
      }
    }

    def is2_0: Boolean = {
      sparkVersion match {
        case versionRegex(major, minor) if major.toInt == 2 && minor.toInt == 0 => true
        case _ => false
      }
    }

  }
}
