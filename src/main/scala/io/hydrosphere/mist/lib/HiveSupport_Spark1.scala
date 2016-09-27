package io.hydrosphere.mist.lib

import io.hydrosphere.mist.contexts.ContextWrapper
import org.apache.spark.sql.hive.HiveContext

trait HiveSupport extends ContextSupport {
  private var _hiveContext: HiveContext = null

  protected def hiveContext: HiveContext = _hiveContext

  override private[mist] def setup(sc: ContextWrapper): Unit = {
    super.setup(sc)
    _hiveContext = sc.hiveContext
  }
}
