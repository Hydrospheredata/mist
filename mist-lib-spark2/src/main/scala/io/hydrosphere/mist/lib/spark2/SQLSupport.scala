package io.hydrosphere.mist.lib.spark2

import org.apache.spark.sql.SparkSession

trait SQLSupport extends SessionSupport {

  private var _session: SparkSession = _

  override def session: SparkSession = _session

  override private[mist] def setup(sc: ContextWrapper): Unit = {
    super.setup(sc)
    _session = sc.sparkSession
  }
}
