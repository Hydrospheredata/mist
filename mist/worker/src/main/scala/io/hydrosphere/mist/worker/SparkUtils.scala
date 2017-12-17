package io.hydrosphere.mist.worker

import org.apache.spark.SparkContext

object SparkUtils {

  def getSparkUiAddress(sc: SparkContext): Option[String] = {
    sc.uiWebUrl
  }


}
