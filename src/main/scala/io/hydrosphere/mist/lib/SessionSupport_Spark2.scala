package io.hydrosphere.mist.lib

import org.apache.spark.sql.SparkSession

//noinspection ScalaFileName
trait SessionSupport extends ContextSupport {

  def session: SparkSession

}