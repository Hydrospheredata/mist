package io.hydrosphere.mist.api

import org.apache.spark.sql.hive.HiveContext

trait HiveSupport extends ContextSupport {

  private var _hiveContext: HiveContext = _

  protected def hiveContext: HiveContext = _hiveContext

  override private[mist] def setup(conf: SetupConfiguration): Unit = {
    super.setup(conf)
    _hiveContext = new HiveContext(conf.context)
  }
}
