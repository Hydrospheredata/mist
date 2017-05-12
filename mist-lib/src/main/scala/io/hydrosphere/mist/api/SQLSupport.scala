package io.hydrosphere.mist.api

import org.apache.spark.sql.SQLContext

trait SQLSupport extends ContextSupport {
  private var _sqlContext: SQLContext = _

  protected def sqlContext: SQLContext = _sqlContext

  override private[mist] def setup(conf: SetupConfiguration): Unit = {
    super.setup(conf)
    _sqlContext = new SQLContext(conf.context)
  }
}
