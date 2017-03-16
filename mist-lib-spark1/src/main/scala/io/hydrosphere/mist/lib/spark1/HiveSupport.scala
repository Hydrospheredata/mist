package io.hydrosphere.mist.lib.spark1

import org.apache.spark.sql.hive.HiveContext

trait HiveSupport extends ContextSupport {

  private var _hiveContext: HiveContext = _

  protected def hiveContext: HiveContext = _hiveContext

  override private[mist] def setup(sc: ContextWrapper): Unit = {
    super.setup(sc)
    _hiveContext = sc.hiveContext
  }
}
