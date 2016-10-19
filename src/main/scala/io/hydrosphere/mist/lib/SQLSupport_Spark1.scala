package io.hydrosphere.mist.lib

import io.hydrosphere.mist.contexts.ContextWrapper
import org.apache.spark.sql.SQLContext

trait SQLSupport extends ContextSupport {
  private var _sqlContext: SQLContext = null

  protected def sqlContext: SQLContext = _sqlContext

  override private[mist] def setup(sc: ContextWrapper): Unit = {
    super.setup(sc)
    _sqlContext = sc.sqlContext
  }
}
