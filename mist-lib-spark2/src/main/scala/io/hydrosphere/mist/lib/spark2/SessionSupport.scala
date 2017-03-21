package io.hydrosphere.mist.lib.spark2

import org.apache.spark.sql.SparkSession

trait SessionSupport extends ContextSupport {

  private var _session: SparkSession = _

  def session: SparkSession = _session

  override private[mist] def setup(conf: SetupConfiguration): Unit = {
    super.setup(conf)
    val enableHive = this.isInstanceOf[HiveSupport]

    var builder = SparkSession
      .builder()
      .appName(conf.context.appName)
      .config(conf.context.getConf)

    if (enableHive) {
      builder = builder.enableHiveSupport()
    }

    _session = builder.getOrCreate()
  }
}
