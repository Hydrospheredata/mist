package io.hydrosphere.mist.lib.spark2

import org.apache.spark.sql.SparkSession

trait SessionSupport extends ContextSupport {

  def session: SparkSession

}
