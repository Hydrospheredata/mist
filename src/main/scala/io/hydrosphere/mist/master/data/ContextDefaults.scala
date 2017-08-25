package io.hydrosphere.mist.master.data

import io.hydrosphere.mist.master.{ContextsSettings, MasterConfig}
import io.hydrosphere.mist.master.models.ContextConfig

/**
  * Created by blvp on 25.08.17.
  */
class ContextDefaults(mistConfigPath: String) {
  def defaultConfig: ContextConfig = defaultSettings.default

  private[data] def defaultSettings: ContextsSettings = {
    val cfg = MasterConfig.loadConfig(mistConfigPath)
    ContextsSettings(cfg.getConfig("mist"))
  }

  private[data] def defaultsMap: Map[String, ContextConfig] = {
    val ds = defaultSettings
    import ds._
    contexts + (default.name -> default)
  }

}
