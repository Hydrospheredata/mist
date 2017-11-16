package io.hydrosphere.mist.worker

import org.apache.spark.SparkContext

object SparkUtils {

  def getSparkUiAddress(sc: SparkContext): Option[String] = {
    if (org.apache.spark.SPARK_VERSION.startsWith("1.")) {}
    val getUI = sc.getClass.getDeclaredMethod("ui")
    getUI.setAccessible(true)
    val maybeUI = getUI.invoke(sc).asInstanceOf[Option[AnyRef]]
    maybeUI.map(sparkUI => {
      val mth = sparkUI.getClass.getDeclaredMethod("appUIAddress")
      mth.setAccessible(true)
      mth.invoke(sparkUI).asInstanceOf[String]
    })
  }


}
